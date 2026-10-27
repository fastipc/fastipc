using System;

#nullable enable

namespace FastIpc
{
    /// <summary>
    /// A setup call failed: a create, connect or join (<see cref="FipcStream"/>, <see cref="FipcBroadcast"/>,
    /// <see cref="FipcQueue"/>, <see cref="FipcService"/>) or <see cref="FipcListener.Accept(int)"/>.
    /// <see cref="Result"/> says why, as the raw layer would return it. Nothing after setup throws it: the calls on a
    /// connection, a queue or a service return a <see cref="FipcResult"/>, since a timeout, the peer's end and the
    /// others are normal events of their loops.
    /// </summary>
    public sealed class FipcException : Exception
    {
        internal FipcException(string operation, string name, FipcResult result)
            : base($"{operation} '{name}': {Fipc.ResultStr(result)}") => Result = result;

        internal FipcException(string operation, FipcResult result)
            : base($"{operation}: {Fipc.ResultStr(result)}") => Result = result;

        /// <summary>The call's result: never <see cref="FipcResult.Ok"/>.</summary>
        public FipcResult Result { get; }
    }
}
