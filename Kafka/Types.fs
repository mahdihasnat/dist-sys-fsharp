[<AutoOpen>]
module Kafka.Types


open System
open FSharpPlus
open FSharpPlus.Data
open Types
open Fleece


type Value = Value of int
with
    static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Value x) -> x) Value Codecs.int

type LogKey = LogKey of string
with
    static member get_Codec () : Codec<'a, LogKey> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (LogKey x) -> x) LogKey Codecs.string

    member this.Value =
        match this with
        | LogKey value -> value

type LogValue = LogValue of int
with
    static member get_Codec () : Codec<'a, LogValue> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (LogValue x) -> x) LogValue Codecs.int

    member this.Value =
        match this with
        | LogValue value -> value

type Offset = Offset of int
with
    static member get_Codec () : Codec<'a, Offset> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Offset x) -> x) Offset Codecs.int

    member this.Value =
        match this with
        | Offset value -> value

module Codecs =
    let propMapOfLogKey (codec: Codec<'Encoding, 'a>) = (Ok << Map.ofSeq << Seq.map (Tuple2.mapItem1 LogKey) <<PropertyList.ToSeq <-> (Map.toArray >> Array.map (Tuple2.mapItem1 (fun (LogKey x) -> x)) >> PropertyList)) >.> Codecs.propList codec

[<RequireQualifiedAccess>]
type InputMessageBody =
    | Send of MessageId * LogKey * LogValue
    | Poll of MessageId * Offsets: Map<LogKey, Offset>
    | CommitOffsets of MessageId * Offsets: Map<LogKey, Offset>
    | ListCommittedOffsets of MessageId * NonEmptyList<LogKey>
    | KVResponse of KVResponseMessageBody<Value>
with
    static member OfJson json =
        match json with
        | JObject o ->
            monad {
                let! msgType = jget o "type"
                match msgType with
                | s when s = "send" ->
                    let! messageId = jget o "msg_id"
                    let! key = jget o "key"
                    let! msg = jget o "msg"
                    return Send(messageId, key, msg)
                | s when s = "poll" ->
                    let! messageId = jget o "msg_id"
                    let! offsets = jgetWith (Codec.decode (Codecs.propMapOfLogKey defaultCodec<_, Offset>)) o "offsets"
                    return Poll(messageId, offsets)
                | s when s = "commit_offsets" ->
                    let! messageId = jget o "msg_id"
                    let! offsets = jgetWith (Codec.decode (Codecs.propMapOfLogKey defaultCodec<_, Offset>)) o "offsets"
                    return CommitOffsets(messageId, offsets)
                | s when s = "list_committed_offsets" ->
                    let! messageId = jget o "msg_id"
                    let! keys = jget o "keys"
                    return ListCommittedOffsets(messageId, keys)
                | _ ->
                    let! kvResponse = KVResponseMessageBody<Value>.OfJson json
                    return KVResponse kvResponse
            }
        | x ->
            Decode.Fail.objExpected x

type OutputMessageBody =
    | SendAck of InReplyTo: MessageId * Offset
    | PollAck of InReplyTo: MessageId * Messages: Map<LogKey, List<Offset * LogValue>>
    | CommitOffsetsAck of InReplyTo: MessageId
    | ListCommittedOffsetsAck of InReplyTo: MessageId * Offsets: Map<LogKey, Offset>
    | KVRequest of KVRequestMessageBody<Value>
with
    static member ToJson (x: OutputMessageBody) =
        match x with
        | SendAck(inReplyTo, offset) ->
            jobj [
                "type" .= "send_ok"
                "in_reply_to" .= inReplyTo
                "offset" .= offset
            ]
        | PollAck(inReplyTo, messages) ->
            jobj [
                "type" .= "poll_ok"
                "in_reply_to" .= inReplyTo
                map (Codec.encode (Codecs.propMapOfLogKey defaultCodec<_, List<Offset * LogValue>>)) ("msgs", messages)
            ]
        | CommitOffsetsAck inReplyTo ->
            jobj [
                "type" .= "commit_offsets_ok"
                "in_reply_to" .= inReplyTo
            ]
        | ListCommittedOffsetsAck(inReplyTo, offsets) ->
            jobj [
                "type" .= "list_committed_offsets_ok"
                "in_reply_to" .= inReplyTo
                map (Codec.encode (Codecs.propMapOfLogKey defaultCodec<_, Offset>)) ("offsets", offsets)
            ]
        | KVRequest kvRequestMessageBody ->
            KVRequestMessageBody<Value>.ToJson kvRequestMessageBody


type Node = {
    Info : InitialNodeInfo
    NextMessageId: int

    CachedMessages: Map<LogKey, Map<Offset, LogValue>>

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
and TransitionResult = Node * List<Message<OutputMessageBody>>