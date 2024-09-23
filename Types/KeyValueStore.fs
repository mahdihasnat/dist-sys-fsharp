[<AutoOpen>]
module Types.KeyValueStore

open FSharpPlus
open Fleece
open System.Text.RegularExpressions
open Types

type Value = Value of int
with
    static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Value x) -> x) Value Codecs.int

[<RequireQualifiedAccess>]
type KVRequestMessageBody<'Value> =
    | Read of MessageId * Key: string
    | CompareAndSwap of MessageId * Key: string * From: 'Value * To: 'Value * CreateIfNotExists: bool
with
    static member inline ToJson (x: KVRequestMessageBody<_>) =
            match x with
            | KVRequestMessageBody.Read (messageId, key) ->
                jobj [
                    "type" .= "read"
                    "msg_id" .= messageId
                    "key" .= key
                ]
            | KVRequestMessageBody.CompareAndSwap (messageId, key, from, ``to``, createIfNotExists) ->
                jobj [
                    "type" .= "cas"
                    "msg_id" .= messageId
                    "key" .= key
                    "from" .= from
                    "to" .= ``to``
                    "create_if_not_exists" .= createIfNotExists
                ]

[<RequireQualifiedAccess>]
type KVResponseMessageBody =
    | ReadOk of InReplyTo: MessageId * Value: Value
    | ErrorKeyDoesNotExist of InReplyTo: MessageId
    | CompareAndSwapOk of InReplyTo: MessageId
    | ErrorPreconditionFailed of InReplyTo: MessageId * ActualValue: Value
with
    static member inline OfJson json =
            match json with
            | JObject o ->
                monad {
                    let! (msgType: string) = jget o "type"
                    and! (inReplyTo: MessageId) = jget o "in_reply_to"
                    match msgType with
                    | s when s = "read_ok" ->
                        let! value = jget o "value"
                        return KVResponseMessageBody.ReadOk (inReplyTo, value)
                    | s when s = "cas_ok" ->
                        return KVResponseMessageBody.CompareAndSwapOk (inReplyTo)
                    | s when s = "error" ->
                        let! (code: int) = jget o "code"
                        and! (text: string) = jget o "text"
                        match code with
                        | 20 ->
                            assert (text = "key does not exist")
                            return KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo
                        | 22 ->
                            let pattern = @"^current value (-?\d+) is not (-?\d+)$"
                            let ``match`` = Regex.Match(text, pattern)
                            assert ``match``.Success
                            let firstNumber = int ``match``.Groups.[1].Value
                            return KVResponseMessageBody.ErrorPreconditionFailed (inReplyTo, Value firstNumber)
                        | _ ->
                            return! Error <| DecodeError.Uncategorized $"Error code is not 20 or 22, code = {code}"
                    | _ ->
                        return! Error <| DecodeError.Uncategorized $"Message type is not supported: {msgType}"
                }
            | x ->
                Decode.Fail.objExpected x

