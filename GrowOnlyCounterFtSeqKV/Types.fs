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

type SeqKVReadRPCError = SeqKVReadRPCError of unit

type KeyDoesNotExist = KeyDoesNotExist of unit

type InputMessageBody =
    | Add of MessageId * Delta
    | Read of MessageId
    | OnSeqKVReadOk of InReplyTo: MessageId * Value
    | OnSeqKVReadKeyDoesNotExist of InReplyTo: MessageId
    | OnSeqKVCompareAndSwapOk of InReplyTo: MessageId
    | OnSeqKVCompareAndSwapPreconditionFailed of InReplyTo: MessageId
with
    static member get_Codec () =
        codec {
            let! msgType = jreqAlways "type" (function | Add _ -> "add" | Read _ -> "read" | OnSeqKVReadOk _ -> "read_ok" | OnSeqKVReadKeyDoesNotExist _ -> "error" | OnSeqKVCompareAndSwapOk _ -> "cas_ok" | OnSeqKVCompareAndSwapPreconditionFailed _ -> "error")
            and! messageId = jopt "msg_id" (function | Add (messageId, _) | Read messageId -> Some messageId | _ -> None)
            and! delta = jopt "delta" (function | Add (_, delta) -> Some delta | _ -> None)
            and! inReplyTo = jopt "in_reply_to" (function | OnSeqKVReadOk (inReplyTo, _) | OnSeqKVReadKeyDoesNotExist inReplyTo | OnSeqKVCompareAndSwapOk inReplyTo | OnSeqKVCompareAndSwapPreconditionFailed inReplyTo -> Some inReplyTo | _ -> None)
            and! value = jopt "value" (function | OnSeqKVReadOk (_, value) -> Some value | _ -> None)
            and! code = jopt "code" (function | OnSeqKVReadKeyDoesNotExist _ -> Some 20 | OnSeqKVCompareAndSwapPreconditionFailed _ -> Some 22 | _ -> None)
            return
                match msgType with
                | s when s = "add" ->
                    Add (messageId |> Option.get, delta |> Option.get)
                | s when s = "read" ->
                    Read (messageId |> Option.get)
                | s when s = "read_ok" ->
                    OnSeqKVReadOk (inReplyTo |> Option.get, value |> Option.get)
                | s when s = "error" ->
                    if code |> Option.get = 20 then
                        OnSeqKVReadKeyDoesNotExist (inReplyTo |> Option.get)
                    elif code |> Option.get = 22 then
                        OnSeqKVCompareAndSwapPreconditionFailed (inReplyTo |> Option.get)
                    else
                        failwithf $"Invalid error code: %d{code |> Option.get}"
                | s when s = "cas_ok" ->
                    OnSeqKVCompareAndSwapOk (inReplyTo |> Option.get)
                | _ ->
                    failwithf $"Invalid message type: %s{msgType}"

        }
        |> ofObjCodec

type OutputMessageBody =
    | AddAck of InReplyTo: MessageId
    | ReadAck of InReplyTo: MessageId * Value
    | SeqKVRead of MessageId * Key: string
    | SeqKVCompareAndSwap of MessageId * Key: string * From: Value * To :Value * CreateIfNotExists: bool
with
    static member get_Codec () =
        codec {
            let! msgType = jreqAlways "type" (function | AddAck _ -> "add_ok" | ReadAck _ -> "read_ok" | SeqKVRead _ -> "read" | SeqKVCompareAndSwap _ -> "cas")
            and! messageId = jopt "msg_id" (function | SeqKVRead (messageId, _) | SeqKVCompareAndSwap (messageId, _, _, _, _)  -> Some messageId| _ -> None)
            and! inReplyTo = jopt "in_reply_to" (function | ReadAck (inReplyTo, _) -> Some inReplyTo | _ -> None)
            and! value = jopt "value" (function | ReadAck (_, value) -> Some value | _ -> None)
            and! key = jopt "key" (function | SeqKVRead (_, key) | SeqKVCompareAndSwap (_, key, _, _, _) -> Some key | _ -> None)
            and! from = jopt "from" (function | SeqKVCompareAndSwap (_, _, from, _, _) -> Some from | _ -> None)
            and! ``to`` = jopt "to" (function | SeqKVCompareAndSwap (_, _, _, ``to``, _) -> Some ``to`` | _ -> None)
            and! createIfNotExists = jopt "create_if_not_exists" (function | SeqKVCompareAndSwap (_, _, _, _, createIfNotExists) -> Some createIfNotExists | _ -> None)

            return
                match msgType with
                | s when s = "add_ok" ->
                    AddAck (inReplyTo |> Option.get)
                | s when s = "read_ok" ->
                    ReadAck (inReplyTo |> Option.get, value |> Option.get)
                | s when s = "read" ->
                    SeqKVRead (messageId |> Option.get, key |> Option.get)
                | s when s = "cas" ->
                    SeqKVCompareAndSwap (messageId |> Option.get, key |> Option.get, from |> Option.get, ``to`` |> Option.get, createIfNotExists |> Option.get)
                | _ ->
                    failwithf $"Invalid message type: %s{msgType}"
        }
        |> ofObjCodec

type Node = {
    Info: InitialNodeInfo
    NextMessageId: int
    OnSeqKVReadOkHandlers : Map<MessageId, Node -> Value -> Node * List<Message<OutputMessageBody>>>
    OnSeqKVReadKeyDoesNotExistHandlers : Map<MessageId, Node -> Node * List<Message<OutputMessageBody>>>
    OnSeqKVCompareAndSwapOkHandlers : Map<MessageId, Node -> Node * List<Message<OutputMessageBody>>>
}