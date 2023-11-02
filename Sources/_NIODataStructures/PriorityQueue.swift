//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2017-2021 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

public struct PriorityQueue<Element: Comparable> {
    
    internal var _heap: Heap<Element>

    
    public init() {
        self._heap = Heap()
    }

    
    public mutating func remove(_ key: Element) {
        self._heap.remove(value: key)
    }
    
    
    public mutating func removeFirst(where shouldBeRemoved: (Element) throws -> Bool) rethrows {
        try self._heap.removeFirst(where: shouldBeRemoved)
    }

    
    public mutating func push(_ key: Element) {
        self._heap.append(key)
    }

    
    public func peek() -> Element? {
        return self._heap.storage.first
    }

    
    public var isEmpty: Bool {
        return self._heap.storage.isEmpty
    }

    
    @discardableResult
    public mutating func pop() -> Element? {
        return self._heap.removeRoot()
    }

    
    public mutating func clear() {
        self._heap = Heap()
    }
}

extension PriorityQueue: Equatable {
    
    public static func ==(lhs: PriorityQueue, rhs: PriorityQueue) -> Bool {
        return lhs.count == rhs.count && lhs.elementsEqual(rhs)
    }
}

extension PriorityQueue: Sequence {
    public struct Iterator: IteratorProtocol {

        /* private but */ 
        var _queue: PriorityQueue<Element>

        /* fileprivate but */ 
        public init(queue: PriorityQueue<Element>) {
            self._queue = queue
        }

        
        public mutating func next() -> Element? {
            return self._queue.pop()
        }
    }

    
    public func makeIterator() -> Iterator {
        return Iterator(queue: self)
    }
}

extension PriorityQueue {
    
    public var count: Int {
        return self._heap.count
    }
}

extension PriorityQueue: CustomStringConvertible {
    
    public var description: String {
        return "PriorityQueue(count: \(self.count)): \(Array(self))"
    }
}

extension PriorityQueue: Sendable where Element: Sendable {}
extension PriorityQueue.Iterator: Sendable where Element: Sendable {}
