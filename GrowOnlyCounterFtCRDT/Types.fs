[<AutoOpen>]
module GrowOnlyCounter.Types

open System
open FSharpPlus
open Types
open Fleece

type Delta =
    | Delta of int

    static member get_Codec () : Codec<'a, Delta> when 'a :> IEncoding and 'a: (new: unit -> 'a) =
        Codec.isomorph (fun (Delta x) -> x) Delta Codecs.int

type Value =
    | Value of int

    static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a: (new: unit -> 'a) =
        Codec.isomorph (fun (Value x) -> x) Value Codecs.int

    static member inline (+) (Value x, Delta y) : Value = Value (x + y)
    static member inline (+) (Delta x, Value y) : Value = Value (x + y)
    static member inline (+) (Value x, Value y) : Value = Value (x + y)
    static member inline Zero: Value = Value 0

type Version =
    | Version of int

    static member get_Codec () : Codec<'a, Version> when 'a :> IEncoding and 'a: (new: unit -> 'a) =
        Codec.isomorph (fun (Version x) -> x) Version Codecs.int

    static member inline (+) (Version x, y: int) : Version = Version (x + y)

[<RequireQualifiedAccess>]
type InputMessageBody =
    | Add of MessageId * Delta
    | Read of MessageId
    | Replicate of Crdt: Map<NodeId, Value>

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
                | s when s = "replicate" ->
                    let! crdt = jget o "crdt"
                    return Replicate crdt
                | _ -> return failwithf $"Unknown message type: {msgType}"
            }
        | x -> Decode.Fail.objExpected x

type OutputMessageBody =
    | AddAck of InReplyTo: MessageId
    | ReadAck of InReplyTo: MessageId * Value
    | Replicate of Map<NodeId, Value>

    static member inline ToJson (x: OutputMessageBody) =
        match x with
        | AddAck inReplyTo -> jobj [ "type" .= "add_ok"; "in_reply_to" .= inReplyTo ]
        | ReadAck (inReplyTo, value) -> jobj [ "type" .= "read_ok"; "in_reply_to" .= inReplyTo; "value" .= value ]
        | Replicate crdt -> jobj [ "type" .= "replicate"; "crdt" .= crdt ]

type Node =
    {
        Info: InitialNodeInfo
        Crdt: Map<NodeId, Value>
    }

and TransitionResult = GenericTransitionResult<Node, Message<OutputMessageBody>>
