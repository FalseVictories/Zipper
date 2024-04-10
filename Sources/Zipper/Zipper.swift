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
    var createFolder: (String) -> Void
    var beginWritingFile: (String) -> Void
    var writeData: (Data, Int64) -> Void
    var endWritingFile: () -> Void
    var didFinish: () -> Void
    var errorDidOccur: (ZipperError) -> Void
    
    public init(createFolder: @escaping (String) -> Void, 
                beginWritingFile: @escaping (String) -> Void,
                writeData: @escaping (Data, Int64) -> Void,
                endWritingFile: @escaping () -> Void,
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

final public actor Zipper: Sendable {
    static let LocalHeaderSize = 26
    static let bufferSize = 32_768
    
    enum State {
        case none
        case fillingBuffer((Data) -> Void)
        case readFileName
        case skipBytes(() -> Void)
        case decompressData
        case finished
    }
    
    enum HeaderType {
        case none
        case localFile
        case dataDescriptor
        case archiveExtraData
        case centralDirectory
    }
    
    struct LocalFileHeader {
        let compression: UInt16
        let flags: UInt16
        let dataLength: UInt32
        let decompressedLength: UInt32
        let fileNameLength: UInt16
        let extraDataLength: UInt16
    }
    
    var currentState: State = .none
    
    var dataBuffer: Data = Data()
    
    var decompressionPreBuffer = RingBuffer()
    var decompressionBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: Zipper.bufferSize)
    var decompressionPosition: Int = 0
    
    var bufferFillPosition: Int = 0
    var bytesToFill: Int = 0
    var bytesToSkip: Int = 0
    
    var currentHeader: LocalFileHeader?
    var filename: String = ""
    
    var delegate: ZipperDelegate
    
    var destinationBufferPointer: UnsafeMutablePointer<UInt8>?
    var streamPointer: UnsafeMutablePointer<compression_stream>?
    
    var bytesDownloaded: Int64 = 0
    
    var bytesInFile: Int64 = 0
    
    var decompressionFunction: (UInt8) -> Void
    
    public init(delegate: ZipperDelegate) {
        self.delegate = delegate
        decompressionFunction = { _ in
            fatalError("decompressionFunction set to default") // FIXME: This should throw
        }
    }
    
    public func consume<S: AsyncSequence>(_ iterator: S) async throws where S.Element == UInt8 {
        currentState = .fillingBuffer(parsePKEntryHeader(from:))
        bytesToFill = 4
        bufferFillPosition = 0

    byteIterator:
        for try await byte in iterator {
            bytesDownloaded += 1
            
            switch currentState {
            case .none:
                delegate.errorDidOccur(.invalidState)
                return
            
            case .fillingBuffer(let completion):
                if bytesToFill > 0 {
                    dataBuffer.insert(byte, at: bufferFillPosition)
                    bufferFillPosition += 1
                    bytesToFill -= 1
                }
                
                if bytesToFill == 0 {
                    currentState = .none
                    completion(dataBuffer)
                }
                
                break

            case .readFileName:
                guard let currentHeader else {
                    delegate.errorDidOccur(.invalidArchive)
                    return
                }
                
                if bytesToFill > 0 {
                    filename.append(Character(UnicodeScalar(byte)))
                    bufferFillPosition += 1
                    bytesToFill -= 1
                }
                
                if bytesToFill == 0 {
                    bytesToSkip = Int(currentHeader.extraDataLength)
                    currentState = .skipBytes(decideWhatToDoAfterSkippingExtraData)
                }
                break
                
            case .skipBytes(let completion):
                if bytesToSkip > 0 {
                    bytesToSkip -= 1
                }
                
                if bytesToSkip == 0 {
                    currentState = .none
                    completion()
                }
                break
                
            case .decompressData:
                decompressionFunction(byte)
                break
                
            case .finished:
                break byteIterator
            }
        }
        
        decompressionBuffer.deallocate()
        delegate.didFinish()
    }
}

private extension Zipper {
    func resetForNextEntry() {
        filename = ""
        dataBuffer.removeAll()
        
        bufferFillPosition = 0
        bytesToFill = 4
        bytesInFile = 0
        
        currentState = .fillingBuffer(parsePKEntryHeader(from:))
        currentHeader = nil
    }
    
    func fillLocalHeader() {
        bytesToFill = 26
        bufferFillPosition = 0
        currentState = .fillingBuffer(parseLocalFileHeader(from:))
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
    
    func parsePKEntryHeader(from data: Data) {
        guard data.count == 4 else {
            return
        }
        
        let headerValue = extractUInt32(from: data, at: 0)
        switch parsePKHeader(headerValue) {
        case .none:
            currentState = .finished
            break
            
        case .localFile:
            fillLocalHeader()
            break
            
        case .dataDescriptor:
            skipDataDescriptor()
            break
            
        case .archiveExtraData:
            currentState = .finished
            break
            
        case .centralDirectory:
            currentState = .finished
            break
        }
    }
    
    func skipDataDescriptor() {
        bytesToSkip = 12
        currentState = .skipBytes(resetForNextEntry)
    }
    
    func parseLocalFileHeader(from data: Data) {
        let flags = extractUInt16(from: dataBuffer, at: 2)
        let compression = extractUInt16(from: dataBuffer, at: 4)
        let dataLength = extractUInt32(from: dataBuffer, at: 14)
        let decompressedLength = extractUInt32(from: dataBuffer, at: 18)
        let filenameLength = extractUInt16(from: dataBuffer, at: 22)
        let extraHeaderLength = extractUInt16(from: dataBuffer, at: 24)
        
        currentHeader = LocalFileHeader(compression: compression,
                                        flags: flags,
                                        dataLength: dataLength,
                                        decompressedLength: decompressedLength,
                                        fileNameLength: filenameLength,
                                        extraDataLength: extraHeaderLength)
        
        dataBuffer.removeAll()
        bytesToFill = Int(filenameLength)
        bufferFillPosition = 0
        
        filename = ""
        currentState = .readFileName
        
        decompressionFunction = dataLength == 0 ? extractLengthUnknown : extractLengthKnown
    }
    
    func decideWhatToDoAfterSkippingExtraData() {
        guard let currentHeader else {
            delegate.errorDidOccur(.invalidArchive)
            return
        }
        
        if currentHeader.dataLength == 0 && currentHeader.flags == 0 {
            delegate.createFolder(filename)
            resetForNextEntry()
            return
        }
        
        // Only need to initialise the decompressor if compression is used
        if currentHeader.compression == 8 {
            if !setUpDecompressor() {
                // Tear down anything that was already set up before failure
                tearDownDecompressor()
                
                delegate.errorDidOccur(.invalidCompressor)
                
                currentState = .finished
                return
            }
        }

        dataBuffer.removeAll()
        delegate.beginWritingFile(filename)
        decompressionPreBuffer.clear()
                
        currentState = .decompressData
    }
    
    func setUpDecompressor() -> Bool {
        destinationBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: Zipper.bufferSize)
        
        streamPointer = UnsafeMutablePointer<compression_stream>.allocate(capacity: 1)
        guard let streamPointer else {
            return false
        }
        
        let status = compression_stream_init(streamPointer, COMPRESSION_STREAM_DECODE, COMPRESSION_ZLIB)
        guard status != COMPRESSION_STATUS_ERROR else {
            return false
        }
        
        streamPointer.pointee.src_size = 0
        streamPointer.pointee.dst_ptr = destinationBufferPointer!
        streamPointer.pointee.dst_size = Zipper.bufferSize
        
        return true
    }
    
    func tearDownDecompressor() {
        if let destinationBufferPointer {
            destinationBufferPointer.deallocate()
        }
        destinationBufferPointer = nil
        
        if let streamPointer {
            compression_stream_destroy(streamPointer)
            streamPointer.deallocate()
        }
        streamPointer = nil
    }
    
    func extractLengthKnown(_ byte: UInt8) {
        guard let currentHeader else {
            return // Should throw?
        }
        
        decompressionBuffer[decompressionPosition] = byte
        decompressionPosition += 1
        bytesInFile += 1
        
        if decompressionPosition >= Zipper.bufferSize || bytesInFile == currentHeader.dataLength {
            let dataBuffer = Data(bytesNoCopy: decompressionBuffer,
                                  count: decompressionPosition,
                                  deallocator: .none)
            if currentHeader.compression == 0 {
                delegate.writeData(dataBuffer, bytesDownloaded)
            } else if currentHeader.compression == 8 {
                decompress(data: dataBuffer, finished: decompressionPosition < Zipper.bufferSize)
            } else {
                delegate.errorDidOccur(.unknownEncryption)
                currentState = .finished
                
                tearDownDecompressor()
                
                return
            }
            
            decompressionPosition = 0
        }

        if bytesInFile == currentHeader.dataLength {
            delegate.endWritingFile()
            tearDownDecompressor()
            
            bytesInFile = 0
            
            currentState = .fillingBuffer(parsePKEntryHeader(from:))
            bytesToFill = 4
            bufferFillPosition = 0
        }
    }
    
    func extractLengthUnknown(_ byte: UInt8) {
        // Take the front of the prebuffer and put it into the decompression buffer
        if decompressionPreBuffer.count == 4 {
            let decompressValue = decompressionPreBuffer.peek()
            decompressionBuffer[decompressionPosition] = decompressValue
            decompressionPosition += 1
            bytesInFile += 1
        }
        
        // push the newest byte into the prebuffer
        decompressionPreBuffer.push(byte)
                        
        // Check if there's a header value that means decompression should end
        let totalValue = decompressionPreBuffer.totalValue
        var headerType = parsePKHeader(totalValue)
        
        if headerType != .none {
            if let currentHeader {
                if currentHeader.dataLength == 0 {
                    if headerType != .dataDescriptor {
                        headerType = .none
                    }
                } else {
                    if bytesInFile != currentHeader.dataLength {
                        headerType = .none
                    }
                }
            }
        }
                        
        // Pass a buffer to the decompressor when it's full or it's the last buffer
        if decompressionPosition >= Zipper.bufferSize || headerType != .none {
            let dataBuffer = Data(bytesNoCopy: decompressionBuffer,
                                  count: decompressionPosition,
                                  deallocator: .none)
            if currentHeader?.compression == 0 {
                delegate.writeData(dataBuffer, bytesDownloaded)
            } else if currentHeader?.compression == 8 {
                decompress(data: dataBuffer, finished: decompressionPosition < Zipper.bufferSize)
            } else {
                delegate.errorDidOccur(.unknownEncryption)
                currentState = .finished
                
                tearDownDecompressor()
                
                return
            }
            
            decompressionPosition = 0
        }
        
        if headerType != .none {
            delegate.endWritingFile()
            tearDownDecompressor()
            
            bytesInFile = 0
            
            switch headerType {
            case .none:
                break
                
            case .localFile:
                fillLocalHeader()
                break
                
            case .dataDescriptor:
                skipDataDescriptor()
                break
                
            case .archiveExtraData:
                currentState = .finished
                break
                
            case .centralDirectory:
                currentState = .finished
                break
            }
        }
    }
    
    func decompress(data buffer: Data, finished: Bool) {
        guard let destinationBufferPointer,
              let streamPointer else {
            return
        }
        
        let count = buffer.count
        var flags = Int32(0)
        
        if finished {
            flags = Int32(COMPRESSION_STREAM_FINALIZE.rawValue)
        }
        
        streamPointer.pointee.src_size = count
        
        // Process everything in the buffer
        var status: compression_status = COMPRESSION_STATUS_OK
        repeat {
            if streamPointer.pointee.src_size == 0 {
                return
            }
            
            buffer.withUnsafeBytes {
                let baseAddress = $0.bindMemory(to: UInt8.self).baseAddress!
                streamPointer.pointee.src_ptr = baseAddress.advanced(by: count - streamPointer.pointee.src_size)
                
                status = compression_stream_process(streamPointer, flags)
            }
            
            switch status {
            case COMPRESSION_STATUS_OK, COMPRESSION_STATUS_END:
                let dataCount = Zipper.bufferSize - streamPointer.pointee.dst_size
                
                let outputData = Data(bytesNoCopy: destinationBufferPointer,
                                      count: dataCount,
                                      deallocator: .none)
                
                delegate.writeData(outputData, bytesDownloaded)
                
                streamPointer.pointee.dst_ptr = destinationBufferPointer
                streamPointer.pointee.dst_size = Zipper.bufferSize
                break
                
            case COMPRESSION_STATUS_ERROR:
                delegate.errorDidOccur(.compressorError)
                break
                
            default:
                break
            }
        } while status == COMPRESSION_STATUS_OK
    }
    
    func extractUInt16(from data: Data, at position: Int) -> UInt16 {
        var value: UInt16 = 0
        value = UInt16(data[position + 1])
        value = value << 8
        value = value | UInt16(data[position])
        return value
    }
    
    func extractUInt32(from data: Data, at position: Int) -> UInt32 {
        var value: UInt32 = 0
        
        value = UInt32(data[position + 3]) << 24
        value = value | UInt32(data[position + 2]) << 16
        value = value | UInt32(data[position + 1]) << 8
        value = value | UInt32(data[position])
        
        return value
    }
}
