[<AutoOpen>]
module TotallyAvailableTransactions.Types

open FSharpPlus.Data
open Types
open Fleece.SystemTextJson
open Fleece.Operators
open FSharpPlus
open Fleece

type Key = Key of int
with
    static member get_Codec () : Codec<'a, Key> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Key x) -> x) Key Codecs.int

type Value = Value of int
with
    static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Value x) -> x) Value Codecs.int

type TransactionOperationInput =
    | Read of Key
    | Write of Key * Value
with
    static member OfJson json =
        match json with
        | JArray arr ->
            monad {
                let! opType = ofJson (arr.Item 0)
                match opType with
                | s when s = "r" ->
                    let! (key: Key) = ofJson (arr.Item 1)
                    let! (maybeNull: Option<unit>) = ofJson (arr.Item 2)
                    match maybeNull with
                    | None ->
                        return Read key
                    | Some _unit ->
                        return!  (Error (DecodeError.Uncategorized $"Unexpected value in read operation: {arr.Item 2}"))
                | s when s = "w" ->
                    let! (key: Key) = ofJson (arr.Item 1)
                    let! (value: Value) = ofJson (arr.Item 2)
                    return Write(key, value)
                | x ->
                    return failwithf $"Unknown operation type: {x}"
            }
        | x ->
            Decode.Fail.arrExpected x

[<RequireQualifiedAccess>]
type InputMessageBody =
    | Txn of MessageId * NonEmptyList<TransactionOperationInput>
    | KVResponse of KVResponseMessageBody<Value>
with
    static member OfJson json =
        match json with
        | JObject o ->
            monad {
                let! msgType = jget o "type"
                match msgType with
                | s when s = "txn" ->
                    let! (messageId: MessageId) = jget o "msg_id"
                    let! ops = jget o "txn"
                    return Txn(messageId, ops)
                | _ ->
                    let! kvResponse = KVResponseMessageBody<Value>.OfJson json
                    return KVResponse kvResponse
            }
        | x ->
            Decode.Fail.objExpected x

type TransactionOperationOutput =
    | Read of Key * Value
    | Write of Key * Value
with
    static member ToJson (x: TransactionOperationOutput) =
        match x with
        | Read(key, value) ->
            JArray [
                toEncoding "r"
                toEncoding key
                toEncoding value
            ]
        | Write(key, value) ->
            JArray [
                toEncoding "w"
                toEncoding key
                toEncoding value
            ]

[<RequireQualifiedAccess>]


type OutputMessageBody =
    | TxnAck of InReplyTo: MessageId * List<TransactionOperationOutput>
    | KVRequest of KVRequestMessageBody<Value>
with
    static member ToJson (x: OutputMessageBody) =
        match x with
        | TxnAck(inReplyTo, ops) ->
            jobj [
                "type" .= "txn_ok"
                "in_reply_to" .= inReplyTo
                "txn" .= ops
            ]
        | KVRequest kvRequestMessageBody ->
            KVRequestMessageBody<Value>.ToJson kvRequestMessageBody


type Node = {
    Info : InitialNodeInfo
    NextMessageId: int

    OnKVReadOkHandlers : Map<MessageId, Node -> Value -> TransitionResult>
    OnKVWriteOkHandlers : Map<MessageId, Node -> TransitionResult>
    OnKVErrorKeyDoesNotExistHandlers : Map<MessageId, Node -> TransitionResult>
    OnKVCompareAndSwapOkHandlers : Map<MessageId, Node -> TransitionResult>
    OnKVErrorPreconditionFailedHandlers : Map<MessageId, Node -> TransitionResult>
}
with
    member this.RegisterReadOkHandler (messageId: MessageId) (handler: Node -> Value -> TransitionResult) =
        { this with OnKVReadOkHandlers = this.OnKVReadOkHandlers.Add (messageId, handler) }

    member this.RegisterWriteOkHandler (messageId: MessageId) (handler: Node -> TransitionResult) =
        { this with OnKVWriteOkHandlers = this.OnKVWriteOkHandlers.Add (messageId, handler) }

    member this.RegisterErrorKeyDoesNotExistHandler (messageId: MessageId) (handler: Node -> TransitionResult) =
        { this with OnKVErrorKeyDoesNotExistHandlers = this.OnKVErrorKeyDoesNotExistHandlers.Add (messageId, handler) }

    member this.RegisterCompareAndSwapOkHandler (messageId: MessageId) (handler: Node -> TransitionResult) =
        { this with OnKVCompareAndSwapOkHandlers = this.OnKVCompareAndSwapOkHandlers.Add (messageId, handler) }

    member this.RegisterErrorPreconditionFailedHandler (messageId: MessageId) (handler: Node -> TransitionResult) =
        { this with OnKVErrorPreconditionFailedHandlers = this.OnKVErrorPreconditionFailedHandlers.Add (messageId, handler) }

    member this.UnregisterAllHandler (messageId: MessageId) =
        {
            this with
                OnKVReadOkHandlers = this.OnKVReadOkHandlers.Remove messageId
                OnKVWriteOkHandlers = this.OnKVWriteOkHandlers.Remove messageId
                OnKVErrorKeyDoesNotExistHandlers = this.OnKVErrorKeyDoesNotExistHandlers.Remove messageId
                OnKVCompareAndSwapOkHandlers = this.OnKVCompareAndSwapOkHandlers.Remove messageId
                OnKVErrorPreconditionFailedHandlers = this.OnKVErrorPreconditionFailedHandlers.Remove messageId
        }

and TransitionResult = Node * List<Message<OutputMessageBody>>