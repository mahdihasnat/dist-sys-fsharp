[<AutoOpen>]
module SingleNodeBroadCast.Program

open System
open Fleece.SystemTextJson
open Types
open Utilities
open SingleNodeBroadcast


let rec processStdin (node: Node) : unit =

    let str = Console.ReadLine ()
    if str = null then
        ()
    else
        eprintfn $"STDIN: {str}"
        let msg =
            match ofJsonText<Message<InputMessageBody>> str with
            | Error e ->
                failwithf $"Failed to parse input message: {e}"
            | Ok msg ->
                msg

        let addOrIgnoreMessage (node: Node) (messages: Set<int>) : Node =
            if node.Messages.IsSupersetOf messages then
                node
            else
                let newMessages = messages - node.Messages
                for neighbor in node.Neighbors do
                    let notifyMessageBody: InputMessageBody =
                        InputMessageBody.Gossip (MessageId node.MessageCounter, newMessages)
                    let notifyMessage: Message<InputMessageBody> =
                        {
                            Source = node.Info.NodeId
                            Destination = neighbor
                            MessageBody = notifyMessageBody
                        }
                    printfn $"{toJsonText notifyMessage}"
                    eprintfn $"STDOUT: {toJsonText notifyMessage}"
                {
                    node with
                        Messages = Set.union node.Messages newMessages
                        MessageCounter = node.MessageCounter + 1
                }
        let node =
            match msg.MessageBody with
            | InputMessageBody.Gossip (messageId, messages) ->
                addOrIgnoreMessage node messages

            | InputMessageBody.BroadCast(messageId, message) ->
                let replyMessageBody: BroadCastReplyMessageBody =
                    {
                        InReplyTo = messageId
                    }
                let replyMessage: Message<BroadCastReplyMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = replyMessageBody
                    }
                printfn $"{toJsonText replyMessage}"
                eprintfn $"STDOUT: {toJsonText replyMessage}"

                addOrIgnoreMessage node (Set.ofSeq [message])

            | InputMessageBody.Read messageId ->
                let replyMessageBody: ReadReplyMessageBody =
                    {
                        Messages = node.Messages
                        InReplyTo = messageId
                    }
                let replyMessage: Message<ReadReplyMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = replyMessageBody
                    }
                printfn $"{toJsonText replyMessage}"
                eprintfn $"STDOUT: {toJsonText replyMessage}"
                node

            | InputMessageBody.Topology(messageId, topology) ->
                let replyMessageBody: TopologyReplyMessageBody =
                    {
                        InReplyTo = messageId
                    }
                let replyMessage: Message<TopologyReplyMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = replyMessageBody
                    }
                printfn $"{toJsonText replyMessage}"
                eprintfn $"STDOUT: {toJsonText replyMessage}"
                {
                    node with
                        Neighbors = topology.TryFind node.Info.NodeId |> Option.defaultValue Set.empty
                }

            | InputMessageBody.GossipAck messageId ->
                node.PendingAck.TryFind messageId
                |> Option.map (fun (nodeId, messages) ->
                    let updatedAckedMessages =
                        node.NeighborAckedMessages.TryFind nodeId
                        |> Option.defaultValue Set.empty
                        |> Set.union messages
                    {
                        node with
                            PendingAck = node.PendingAck.Remove messageId
                            NeighborAckedMessages = node.NeighborAckedMessages.Add (nodeId, updatedAckedMessages)
                    }
                )
                |> Option.defaultValue node

        processStdin node

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()
    processStdin {
        Info = nodeInfo
        Messages = Set.empty
        Neighbors = Set.empty
        MessageCounter = 0
        PendingAck = Map.empty
        NeighborAckedMessages = Map.empty
    }
    0