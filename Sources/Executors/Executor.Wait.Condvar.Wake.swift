public import Property
internal import Synchronizer_Blocking

extension Executor.Wait.Condvar {

    public enum Wake {}
}

extension Executor.Wait.Condvar {

    public var wake: Property<Wake, Executor.Wait.Condvar> {
        Property(self)
    }
}

extension Property where Tag == Executor.Wait.Condvar.Wake, Base == Executor.Wait.Condvar {

    public func callAsFunction() {
        base.sync.signal()
    }

    public func all() {
        base.sync.broadcast()
    }
}
