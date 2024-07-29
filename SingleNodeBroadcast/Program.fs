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
        let node =
            match msg.MessageBody with
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
                {
                    node with
                        Messages = message :: node.Messages
                }
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
                node

        processStdin node

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()
    processStdin {
        Info = nodeInfo
        Messages = []
    }
    0