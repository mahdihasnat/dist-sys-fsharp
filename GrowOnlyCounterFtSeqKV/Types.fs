[<AutoOpen>]
module GrowOnlyCounter.Types


open System
open FSharpPlus
open FSharpPlus.Data
open Types
open Fleece

type Delta = Delta of int
with
    static member get_Codec () : Codec<'a, Delta> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Delta x) -> x) Delta Codecs.int

type Value = Value of int
with
    static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Value x) -> x) Value Codecs.int
    static member inline (+) (Value x, Delta y) : Value =
            Value (x + y)

type Version = Version of int
with
    static member get_Codec () : Codec<'a, Version> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Version x) -> x) Version Codecs.int
    static member inline (+) (Version x, y: int) : Version =
            Version (x + y)

type InputMessageBody =
    | Add of MessageId * Delta
    | Read of MessageId
    | KVResponse of KVResponseMessageBody<Value * Version>
with
    static member OfJson json =
            match json with
            | JObject o ->
                monad {
                    let! msgType = jget o "type"
                    match msgType with
                    | s when s = "add" ->
                        let! delta = jget o "delta"
                        and! messageId = jget o "msg_id"
                        return Add (messageId, delta)
                    | s when s = "read" ->
                        let! messageId = jget o "msg_id"
                        return Read messageId
                    | _ ->
                        let! kVResponse = KVResponseMessageBody<Value>.OfJson json
                        return KVResponse kVResponse
                }
            | x ->
                Decode.Fail.objExpected x

type OutputMessageBody =
    | AddAck of InReplyTo: MessageId
    | ReadAck of InReplyTo: MessageId * Value
    | SeqKVOperation of KVRequestMessageBody<Value * Version>
with
    static member inline ToJson (x: OutputMessageBody) =
            match x with
            | AddAck inReplyTo ->
                jobj [
                    "type" .= "add_ok"
                    "in_reply_to" .= inReplyTo
                ]
            | ReadAck (inReplyTo, value) ->
                jobj [
                    "type" .= "read_ok"
                    "in_reply_to" .= inReplyTo
                    "value" .= value
                ]
            | SeqKVOperation x ->
                KVRequestMessageBody<_>.ToJson x

type Node = {
    Info: InitialNodeInfo
    NextMessageId: int

    OnSeqKVReadOkHandlers : Map<MessageId, Node -> Value * Version -> TransitionResult>
    OnSeqKVReadKeyDoesNotExistHandlers : Map<MessageId, Node -> TransitionResult>
    OnSeqKVCompareAndSwapOkHandlers : Map<MessageId, Node -> TransitionResult>
    OnSeqKVCompareAndSwapPreconditionFailedHandlers : Map<MessageId, Node -> TransitionResult>
}
and TransitionResult = GenericTransitionResult<Node, Message<OutputMessageBody>>



