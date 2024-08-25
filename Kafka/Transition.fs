[<AutoOpen>]
module Kafka.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open FSharpPlus.Lens
open Microsoft.FSharp.Core
open Types
open Kafka


let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =

    match action with
    | Choice2Of2 unit ->
        (node, [])
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | InputMessageBody.Send(messageId, key, value) ->
            let logs = node.Messages.TryFind key |> Option.defaultValue Map.empty
            let offset = Offset logs.Count
            let logs =
                logs.Add (offset, value)
            let node =
                {
                    node with
                        Messages = node.Messages.Add (key, logs)
                }
            let replyMessageBody: OutputMessageBody =
                SendAck (messageId, offset)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.Poll (messageId, offsets) ->
            let messages : Map<LogKey, List<Offset * LogValue>> =
                offsets
                |> Map.choosei (fun key offset ->
                    node.Messages.TryFind key
                    |> Option.map (Map.toList >> List.filter (fun (offset', _) -> offset <= offset'))
                )
            let replyMessageBody: OutputMessageBody =
                PollAck (messageId, messages)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.CommitOffsets(messageId, offsets) ->
            let replyMessageBody: OutputMessageBody =
                CommitOffsetsAck (messageId)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        CommittedOffsets =
                            (node.CommittedOffsets, offsets)
                            ||> Map.fold (fun acc key offset ->
                                match acc.TryFind key with
                                | Some offset' -> acc.Add (key, max offset offset')
                                | None -> acc.Add (key, offset)
                            )
                }
            (node, [ replyMessage ])
        | InputMessageBody.ListCommittedOffsets(messageId, keys) ->
            let offsets =
                keys
                |> NonEmptyList.toList
                |> List.choose (fun key ->
                    node.CommittedOffsets.TryFind key
                    |> Option.map (fun offset -> (key, offset))
                )
                |> Map.ofList
            let replyMessageBody: OutputMessageBody =
                ListCommittedOffsetsAck (messageId, offsets)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.Gossip messageId -> failwith "todo"
        | InputMessageBody.GossipAck messageId -> failwith "todo"

