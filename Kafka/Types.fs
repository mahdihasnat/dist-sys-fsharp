[<AutoOpen>]
module Kafka.Types


open System
open FSharpPlus
open FSharpPlus.Data
open Types
open Fleece

type LogKey = LogKey of string
with
    static member get_Codec () : Codec<'a, LogKey> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (LogKey x) -> x) LogKey Codecs.string
type LogValue = LogValue of int
with
    static member get_Codec () : Codec<'a, LogValue> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (LogValue x) -> x) LogValue Codecs.int
type Offset = Offset of int
with
    static member get_Codec () : Codec<'a, Offset> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (Offset x) -> x) Offset Codecs.int

module Codecs =
    let propMapOfLogKey (codec: Codec<'Encoding, 'a>) = (Ok << Map.ofSeq << Seq.map (Tuple2.mapItem1 LogKey) <<PropertyList.ToSeq <-> (Map.toArray >> Array.map (Tuple2.mapItem1 (fun (LogKey x) -> x)) >> PropertyList)) >.> Codecs.propList codec

[<RequireQualifiedAccess>]
type InputMessageBody =
    | Send of MessageId * LogKey * LogValue
    | Poll of MessageId * Offsets: Map<LogKey, Offset>
    | CommitOffsets of MessageId * Offsets: Map<LogKey, Offset>
    | ListCommittedOffsets of MessageId * NonEmptyList<LogKey>
    | Gossip of MessageId
    | GossipAck of InReplyTo: MessageId
with
    static member get_Codec () =
        codec {
            let! (msgType) = jreqAlways "type" (function | Send _ -> "send" | Poll _ -> "poll" | CommitOffsets _ -> "commit_offsets" | ListCommittedOffsets _ -> "list_committed_offsets" | Gossip _ -> "gossip" | GossipAck _ -> "gossip_ok")
            and! (messageId : Option<MessageId>) = jopt "msg_id" (function | Send (messageId, _, _) | Poll (messageId, _) | CommitOffsets (messageId, _) | ListCommittedOffsets (messageId, _) | Gossip (messageId) -> Some messageId | GossipAck _ -> None)
            and! key = jopt "key" (function | Send (_, key, _) -> Some key | Poll _ | CommitOffsets _ | ListCommittedOffsets _ | Gossip _ | GossipAck _ -> None)
            and! msg = jopt "msg" (function | Send (_, _, msg) -> Some msg | Poll _ | CommitOffsets _ | ListCommittedOffsets _ | Gossip _ | GossipAck _ -> None)
            and! offsets = joptWith (Codecs.option (Codecs.propMapOfLogKey defaultCodec<_, Offset>)) "offsets" (function | Poll (_, offsets) | CommitOffsets (_, offsets) -> Some offsets | Send _ | ListCommittedOffsets _ | Gossip _ | GossipAck _ -> None)
            and! keys = jopt "keys" (function | ListCommittedOffsets (_, keys) -> Some keys | Send _ | Poll _ | CommitOffsets _ | Gossip _ | GossipAck _ -> None)
            and! inReplyTo = jopt "in_reply_to" (function | GossipAck (inReplyTo) -> Some inReplyTo | _ -> None)
            match msgType with
            | s when s = "send" ->
                return Send(messageId |> Option.get, key |> Option.get, msg |> Option.get)
            | s when s = "poll" ->
                eprintfn $"poll::Offsets: {offsets}"
                return Poll(messageId |> Option.get, offsets |> Option.get)
            | s when s = "commit_offsets" ->
                return CommitOffsets(messageId |> Option.get, offsets |> Option.get)
            | s when s = "list_committed_offsets" ->
                return ListCommittedOffsets(messageId |> Option.get, keys |> Option.get)
            | s when s = "gossip" ->
                return Gossip (messageId |> Option.get)
            | s when s = "gossip_ok" ->
                return GossipAck (inReplyTo |> Option.get)
            | _ ->
                eprintfn $"Unknown message type: {msgType}"
                return failwithf $"Unknown message type: {msgType}"
        }
        |> ofObjCodec

type OutputMessageBody =
    | SendAck of InReplyTo: MessageId * Offset
    | PollAck of InReplyTo: MessageId * Messages: Map<LogKey, List<Offset * LogValue>>
    | CommitOffsetsAck of InReplyTo: MessageId
    | ListCommittedOffsetsAck of InReplyTo: MessageId * Offsets: Map<LogKey, Offset>
    | GossipAck of InReplyTo: MessageId
    | Gossip of MessageId: MessageId
with
    static member get_Codec () =
        codec {
            let! msgType = jreqAlways "type" (function | SendAck _ -> "send_ok" | PollAck _ -> "poll_ok" | CommitOffsetsAck _ -> "commit_offsets_ok" | ListCommittedOffsetsAck _ -> "list_committed_offsets_ok" | GossipAck _ -> "gossip_ok" | Gossip _ -> "gossip")
            and! inReplyTo = jopt "in_reply_to" (function | SendAck (inReplyTo, _) | PollAck (inReplyTo, _) | CommitOffsetsAck inReplyTo | ListCommittedOffsetsAck (inReplyTo, _) | GossipAck inReplyTo | Gossip inReplyTo -> Some inReplyTo)
            and! offset = jopt "offset" (function | SendAck (_, offset) -> Some offset | PollAck _ | CommitOffsetsAck _ | ListCommittedOffsetsAck _ | GossipAck _ | Gossip _ -> None)
            and! messages = joptWith (Codecs.option (Codecs.propMapOfLogKey defaultCodec<_, List<Offset*LogValue>>)) "msgs" (function | PollAck (_, messages) -> Some messages | SendAck _ | CommitOffsetsAck _ | ListCommittedOffsetsAck _ | GossipAck _ | Gossip _ -> None)
            and! offsets = joptWith (Codecs.option (Codecs.propMapOfLogKey defaultCodec<_, Offset>)) "offsets" (function | ListCommittedOffsetsAck (_, offsets) -> Some offsets | SendAck _ | PollAck _ | CommitOffsetsAck _ | GossipAck _ | Gossip _ -> None)
            and! messageId = jopt "msg_id" (function | Gossip (messageId) -> Some messageId | SendAck _ | PollAck _ | CommitOffsetsAck _ | ListCommittedOffsetsAck _ | GossipAck _ -> None)
            match msgType with
            | s when s = "send_ok" ->
                return SendAck(inReplyTo |> Option.get, offset |> Option.get)
            | s when s = "poll_ok" ->
                return PollAck(inReplyTo |> Option.get, messages |> Option.get)
            | s when s = "commit_offsets_ok" ->
                return CommitOffsetsAck(inReplyTo |> Option.get)
            | s when s = "list_committed_offsets_ok" ->
                return ListCommittedOffsetsAck(inReplyTo |> Option.get, offsets |> Option.get)
            | s when s = "gossip_ok" ->
                return GossipAck(inReplyTo |> Option.get)
            | s when s = "gossip" ->
                return Gossip(messageId |> Option.get)
            | _ ->
                return failwithf $"invalid msgType {msgType}"
        }
        |> ofObjCodec


type Node = {
    Info : InitialNodeInfo
    Messages: Map<LogKey, Map<Offset, LogValue>>
}
