[<AutoOpen>]
module GrowOnlyCounter.Types


open System
open FSharpPlus
open FSharpPlus.Data
open Types
open Fleece
open System.Text.RegularExpressions

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
    | OnSeqKVCompareAndSwapPreconditionFailed of InReplyTo: MessageId * Value
with
    static member get_Codec () =
        codec {
            let! msgType = jreqAlways "type" (function | Add _ -> "add" | Read _ -> "read" | OnSeqKVReadOk _ -> "read_ok" | OnSeqKVReadKeyDoesNotExist _ -> "error" | OnSeqKVCompareAndSwapOk _ -> "cas_ok" | OnSeqKVCompareAndSwapPreconditionFailed _ -> "error")
            and! messageId = jopt "msg_id" (function | Add (messageId, _) | Read messageId -> Some messageId | _ -> None)
            and! delta = jopt "delta" (function | Add (_, delta) -> Some delta | _ -> None)
            and! inReplyTo = jopt "in_reply_to" (function | OnSeqKVReadOk (inReplyTo, _) | OnSeqKVReadKeyDoesNotExist inReplyTo | OnSeqKVCompareAndSwapOk inReplyTo | OnSeqKVCompareAndSwapPreconditionFailed (inReplyTo, _) -> Some inReplyTo | _ -> None)
            and! value = jopt "value" (function | OnSeqKVReadOk (_, value) -> Some value | _ -> None)
            and! code = jopt "code" (function | OnSeqKVReadKeyDoesNotExist _ -> Some 20 | OnSeqKVCompareAndSwapPreconditionFailed _ -> Some 22 | _ -> None)
            and! text = jopt "text" (function | OnSeqKVReadKeyDoesNotExist _ -> Some "key does not exist" | OnSeqKVCompareAndSwapPreconditionFailed _ -> Some "current value $x is not $x" | _ -> None)
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
                        assert (text = Some "key does not exist")
                        OnSeqKVReadKeyDoesNotExist (inReplyTo |> Option.get)
                    elif code |> Option.get = 22 then
                        let text = text |> Option.get
                        let pattern = @"^current value (\d+) is not (\d+)$"
                        let ``match`` = Regex.Match(text, pattern)
                        assert ``match``.Success
                        let firstNumber = int ``match``.Groups.[1].Value
                        OnSeqKVCompareAndSwapPreconditionFailed (inReplyTo |> Option.get, Value firstNumber)
                    else
                        failwithf $"Invalid error code: %d{code |> Option.get}"
                | s when s = "cas_ok" ->
                    OnSeqKVCompareAndSwapOk (inReplyTo |> Option.get)
                | _ ->
                    failwithf $"Invalid message type: %s{msgType}"

        }
        |> ofObjCodec        

[<RequireQualifiedAccess>]
type KVOutputMessageBody<'Value> =
    | Read of MessageId * Key: string
    | CompareAndSwap of MessageId * Key: string * From: 'Value * To: 'Value * CreateIfNotExists: bool
with
    static member inline ToJson (x: KVOutputMessageBody<_>) =
            match x with
            | KVOutputMessageBody.Read (messageId, key) ->
                jobj [
                    "type" .= "read"
                    "msg_id" .= messageId
                    "key" .= key
                ]
            | KVOutputMessageBody.CompareAndSwap (messageId, key, from, ``to``, createIfNotExists) ->
                jobj [
                    "type" .= "cas"
                    "msg_id" .= messageId
                    "key" .= key
                    "from" .= from
                    "to" .= ``to``
                    "create_if_not_exists" .= createIfNotExists
                ]

type OutputMessageBody =
    | AddAck of InReplyTo: MessageId
    | ReadAck of InReplyTo: MessageId * Value
    | SeqKVOperation of KVOutputMessageBody<Value>
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
                KVOutputMessageBody<_>.ToJson x

type Node = {
    Info: InitialNodeInfo
    NextMessageId: int

    ValueCache: Value

    OnSeqKVReadOkHandlers : Map<MessageId, Node -> Value -> Node * List<Message<OutputMessageBody>>>
    OnSeqKVReadKeyDoesNotExistHandlers : Map<MessageId, Node -> Node * List<Message<OutputMessageBody>>>
    OnSeqKVCompareAndSwapOkHandlers : Map<MessageId, Node -> Node * List<Message<OutputMessageBody>>>
    OnSeqKVCompareAndSwapPreconditionFailedHandlers : Map<MessageId, Node -> Value -> Node * List<Message<OutputMessageBody>>>
}