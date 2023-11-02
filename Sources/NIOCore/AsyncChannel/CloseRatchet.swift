//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2022-2023 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// A helper type that lets ``NIOAsyncChannelAdapterHandler`` and ``NIOAsyncChannelWriterHandler`` collude
/// to ensure that the ``Channel`` they share is closed appropriately.
///
/// The strategy of this type is that it keeps track of which side has closed, so that the handlers can work out
/// which of them was "last", in order to arrange closure.

final class CloseRatchet {
    
    enum State {
        case notClosed(isOutboundHalfClosureEnabled: Bool)
        case readClosed
        case writeClosed
        case bothClosed

        
        mutating func closeRead() -> CloseReadAction {
            switch self {
            case .notClosed:
                self = .readClosed
                return .nothing
            case .writeClosed:
                self = .bothClosed
                return .close
            case .readClosed, .bothClosed:
                preconditionFailure("Duplicate read closure")
            }
        }

        
        mutating func closeWrite() -> CloseWriteAction {
            switch self {
            case .notClosed(let isOutboundHalfClosureEnabled):
                self = .writeClosed

                if isOutboundHalfClosureEnabled {
                    return .closeOutput
                } else {
                    return .nothing
                }
            case .readClosed:
                self = .bothClosed
                return .close
            case .writeClosed, .bothClosed:
                preconditionFailure("Duplicate write closure")
            }
        }
    }

    
    var _state: State

    
    init(isOutboundHalfClosureEnabled: Bool) {
        self._state = .notClosed(isOutboundHalfClosureEnabled: isOutboundHalfClosureEnabled)
    }

    
    enum CloseReadAction {
        case nothing
        case close
    }

    
    func closeRead() -> CloseReadAction {
        return self._state.closeRead()
    }

    
    enum CloseWriteAction {
        case nothing
        case close
        case closeOutput
    }

    
    func closeWrite() -> CloseWriteAction {
        return self._state.closeWrite()
    }
}
