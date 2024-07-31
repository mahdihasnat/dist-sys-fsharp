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

        let addOrIgnoreMessage (node: Node) (message: int) : Node =
            if node.Messages.Contains message then
                node
            else
                for neighbor in node.Neighbors do
                    let notifyMessageBody: InputMessageBody =
                        InputMessageBody.Notify message
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
                        Messages = node.Messages.Add message
                }
        let node =
            match msg.MessageBody with
            | InputMessageBody.Notify message ->
                addOrIgnoreMessage node message

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

                addOrIgnoreMessage node message

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

        processStdin node

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()
    processStdin {
        Info = nodeInfo
        Messages = Set.empty
        Neighbors = Set.empty
    }
    0