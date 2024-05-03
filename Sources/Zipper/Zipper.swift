//
//  Zipper.swift
//  Ohia
//
//  Created by iain on 22/12/2023.
//

import Compression
import Foundation

public enum ZipperError: Error {
    case unknownEncryption
    case invalidArchive
    case invalidState
    case invalidCompressor
    case compressorError
}

// FIXME:
// Marked as @unchecked as I'm not sure how to make it Sendable. None of the data passed into the
// closures should be accessed from multiple threads anyway
public struct ZipperDelegate: @unchecked Sendable {
    var createFolder: (String) throws -> Void
    var beginWritingFile: (String) throws -> Void
    var writeData: (Data, Int64) throws -> Void
    var endWritingFile: (String) throws -> Void
    var didFinish: () -> Void
    var errorDidOccur: (ZipperError) -> Void
    
    public init(createFolder: @escaping (String) -> Void,
                beginWritingFile: @escaping (String) -> Void,
                writeData: @escaping (Data, Int64) -> Void,
                endWritingFile: @escaping (String) -> Void,
                didFinish: @escaping () -> Void,
                errorDidOccur: @escaping (ZipperError) -> Void) {
        self.createFolder = createFolder
        self.beginWritingFile = beginWritingFile
        self.writeData = writeData
        self.endWritingFile = endWritingFile
        self.didFinish = didFinish
        self.errorDidOccur = errorDidOccur
    }
}

struct LocalFileHeader {
    let compression: UInt16
    let flags: UInt16
    let dataLength: UInt32
    let decompressedLength: UInt32
    let fileNameLength: UInt16
    let extraDataLength: UInt16
}

final class Context {
    enum BufferState {
        case none
        case fillWorkBuffer((Context, Data) -> Void)
        case skippingData((Context) throws -> Void)
        case unpackData
        case finished
    }
    
    var currentState: BufferState = .none
    
    var bytesToFillOrSkip: UInt32 = 0
    var workBufferOffset: Int = 0
    var workBuffer: Data?
    
    var currentFileHeader: LocalFileHeader?
    var currentFilename = ""
}

/// Unpack a zip file on the fly
final public actor Zipper: Sendable {
    static let PKHeaderSize: UInt32 = 4
    static let LocalHeaderSize: UInt32 = 26
    static let DataDescriptorSize: UInt32 = 12
    static let BufferSize = 65536
    
    enum HeaderType {
        case none
        case localFile
        case dataDescriptor
        case archiveExtraData
        case centralDirectory
    }
    
    let delegate: ZipperDelegate

    public init(delegate: ZipperDelegate) {
        self.delegate = delegate
    }
    
    public func consume<S: AsyncSequence>(_ iterator: S) async throws where S.Element == Data {
        let context = Context()
        resetForNextEntry(with: context)
        
        for try await buffer in iterator {
            try parseBuffer(buffer, with: context)
        }
    }
    
    public func consume<S: AsyncSequence>(_ iterator: S) async throws where S.Element == UInt8 {
        let context = Context()
        resetForNextEntry(with: context)
        
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: Zipper.BufferSize)
        
        var offset = 0
        for try await byte in iterator {
            buffer[offset] = byte
            offset += 1
            
            if offset >= Zipper.BufferSize {
                let dataBuffer = Data(bytesNoCopy: buffer,
                                      count: offset,
                                      deallocator: .none)
                
                try parseBuffer(dataBuffer, with: context)
                offset = 0
            }
        }
        
        let dataBuffer = Data(bytesNoCopy: buffer,
                              count: offset,
                              deallocator: .none)
        
        try parseBuffer(dataBuffer, with: context)
        offset = 0
    }
}

private extension Zipper {
    func parseBuffer(_ currentBuffer: Data,
                     with context: Context) throws {
        var bufferOffset = 0
        
        while bufferOffset < currentBuffer.count {
            switch context.currentState {
            case .none:
                throw ZipperError.invalidArchive
                
            case .fillWorkBuffer(let parseBuffer):
                bufferOffset = try fillWorkBuffer(from: currentBuffer,
                                                  at: bufferOffset,
                                                  with: context)
                
                if context.bytesToFillOrSkip == 0 {
                    if let workBuffer = context.workBuffer {
                        context.workBufferOffset = 0
                        
                        parseBuffer(context, workBuffer)
                    }
                }
                
            case .skippingData(let moveToNextState):
                bufferOffset = skipData(from: currentBuffer,
                                        at: bufferOffset,
                                        with: context)
                
                if context.bytesToFillOrSkip == 0 {
                    try moveToNextState(context)
                }
                
            case .unpackData:
                bufferOffset = try unpackData(from: currentBuffer,
                                              at: bufferOffset,
                                              with: context)
                
                if context.bytesToFillOrSkip == 0 {
                    try delegate.endWritingFile(context.currentFilename)
                    
                    resetForNextEntry(with: context)
                }
                
            case .finished:
                delegate.didFinish()
                return // FIXME: maybe?
            }
        }
    }
    
    func fillWorkBuffer(from currentBuffer: Data,
                        at offset: Int,
                        with context: Context) throws -> Int {
        guard context.workBuffer != nil else {
            throw ZipperError.invalidState
        }
        
        let bytesFromCurrentBuffer  = Int(min(UInt32(currentBuffer.count - offset),
                                              context.bytesToFillOrSkip))
        for idx in 0..<bytesFromCurrentBuffer {
            context.workBuffer?.insert(currentBuffer[offset + idx], at: context.workBufferOffset)
            context.workBufferOffset += 1
            context.bytesToFillOrSkip -= 1
        }
        
        return offset + bytesFromCurrentBuffer
    }
    
    func skipData(from currentBuffer: Data,
                  at offset: Int,
                  with context: Context) -> Int {
        let bytesFromCurrentBuffer = min(UInt32(currentBuffer.count - offset),
                                         context.bytesToFillOrSkip)
        context.bytesToFillOrSkip -= bytesFromCurrentBuffer
        
        return offset + Int(bytesFromCurrentBuffer)
    }
    
    func unpackData(from currentBuffer: Data,
                    at offset: Int,
                    with context: Context) throws -> Int {
        let bytesFromCurrentBuffer = min(UInt32(currentBuffer.count - offset),
                                         context.bytesToFillOrSkip)
        context.bytesToFillOrSkip -= bytesFromCurrentBuffer
        var buffer: Data
        if bytesFromCurrentBuffer == currentBuffer.count {
            buffer = currentBuffer
        } else {
            buffer = currentBuffer.subdata(in: offset..<Int(offset + Int(bytesFromCurrentBuffer)))
        }
        try delegate.writeData(buffer, 0)
        
        return offset + Int(bytesFromCurrentBuffer)
    }
    
    func handlePKHeader(with context: Context,
                        from workBuffer: Data) {
        let header = extractUInt32(from: workBuffer, at: 0)
        let headerType = parsePKHeader(header)
        
        switch headerType {
        case .none, .archiveExtraData, .centralDirectory:
            context.currentState = .finished
            
        case .localFile:
            fillLocalHeader(with: context)
            
        case .dataDescriptor:
            setupSkipDataDescriptor(with: context)
        }
    }
    
    func resetForNextEntry(with context: Context) {
        context.currentFilename = ""
        context.currentState = .fillWorkBuffer(handlePKHeader)
        context.bytesToFillOrSkip = Zipper.PKHeaderSize
        context.workBuffer = Data()
        context.workBufferOffset = 0
        context.currentFileHeader = nil
    }
        
    func setupFillBuffer(ofSize size: UInt32,
                         with context: Context,
                         parseFunction: @escaping (Context, Data) -> Void) {
        context.bytesToFillOrSkip = size
        context.workBuffer = Data()
        context.workBufferOffset = 0
        context.currentState = .fillWorkBuffer(parseFunction)
    }
    
    func setupSkipBuffer(ofSize size: UInt32,
                         with context: Context,
                         moveToNextStateFunction: @escaping (Context) throws -> Void) {
        context.bytesToFillOrSkip = size
        context.currentState = .skippingData(moveToNextStateFunction)
    }
    
    func setupUnpackBuffer(ofSize size: UInt32,
                           with context: Context) -> Void {
        context.bytesToFillOrSkip = size
        context.currentState = .unpackData
    }
    
    func setupSkipDataDescriptor(with context: Context) {
        context.bytesToFillOrSkip = Zipper.DataDescriptorSize
        context.currentState = .skippingData(resetForNextEntry)
    }

    func fillLocalHeader(with context: Context) {
        setupFillBuffer(ofSize: Zipper.LocalHeaderSize,
                        with: context,
                        parseFunction: parseLocalFileHeader)
    }
    
    func parsePKHeader(_ header: UInt32) -> HeaderType {
        if header & 0x4b50 != 0x4b50 {
            return .none
        }
        
        switch header {
        case 0x04034b50:
            return .localFile
            
        case 0x08074b50:
            return .dataDescriptor
            
        case 0x08064b50:
            return .archiveExtraData
            
        case 0x02014b50:
            return .centralDirectory
            
        default:
            return .none
        }
    }
        
    func parseLocalFileHeader(with context: Context, data: Data) {
        let flags = extractUInt16(from: data, at: 2)
        let compression = extractUInt16(from: data, at: 4)
        let dataLength = extractUInt32(from: data, at: 14)
        let decompressedLength = extractUInt32(from: data, at: 18)
        let filenameLength = extractUInt16(from: data, at: 22)
        let extraHeaderLength = extractUInt16(from: data, at: 24)
        
        context.currentFileHeader = LocalFileHeader(compression: compression,
                                                    flags: flags,
                                                    dataLength: dataLength,
                                                    decompressedLength: decompressedLength,
                                                    fileNameLength: filenameLength,
                                                    extraDataLength: extraHeaderLength)
        
        setupFillBuffer(ofSize: UInt32(filenameLength),
                        with: context,
                        parseFunction: parseFilename)
        context.currentFilename = ""
        
        // FIXME
//        decompressionFunction = dataLength == 0 ? extractLengthUnknown : extractLengthKnown
    }
    
    func parseFilename(with context: Context, buffer: Data) {
        guard let currentHeader = context.currentFileHeader else {
            fatalError("No header")
        }
        
        context.currentFilename = String(decoding: buffer, as: UTF8.self)

        setupSkipBuffer(ofSize: UInt32(currentHeader.extraDataLength),
                        with: context,
                        moveToNextStateFunction: decideWhatToDoAfterSkippingExtraData)
    }
    
    func decideWhatToDoAfterSkippingExtraData(context: Context) throws {
        guard let currentHeader = context.currentFileHeader else {
            // FIXME: throw error
            return
        }
        
        if currentHeader.dataLength == 0 && currentHeader.flags == 0 {
            try delegate.createFolder(context.currentFilename)
            // reset for next entry
            return
        }
        
        try delegate.beginWritingFile(context.currentFilename)
        setupUnpackBuffer(ofSize: currentHeader.dataLength, with: context)
    }
        
    func extractUInt16(from data: Data, at position: Int) -> UInt16 {
        var value: UInt16
        value = UInt16(data[position + 1]) << 8
        value = value | UInt16(data[position])
        return value
    }

    func extractUInt32(from data: Data, at position: Int) -> UInt32 {
        var value: UInt32 = 0
        
        value = UInt32(data[position + 3]) << 24
        value = value | UInt32(data[position + 2]) << 16
        value = value | UInt32(data[position + 1]) << 8
        value = value | UInt32(data[position + 0])
        
        return value
    }
}
