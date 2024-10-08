[<AutoOpen>]
module Utilities.Initializer

open Types
open System
open Fleece.SystemTextJson

let initNode () : InitialNodeInfo =
    let strLine = Console.ReadLine ()

    if strLine = null then
        failwith "No input"

    eprintfn $"STDIN: {strLine}"

    let initMessage =
        match ofJsonText<Message<InitMessage>> strLine with
        | Error e -> failwith $"{e}"
        | Ok msg -> msg

    let initReply: InitReplyMessage =
        {
            InReplyTo = initMessage.MessageBody.MessageId
        }

    let replyMessage: Message<InitReplyMessage> =
        {
            Source = initMessage.MessageBody.NodeId
            Destination = initMessage.Source
            MessageBody = initReply
        }

    printfn $"{toJsonText replyMessage}"
    eprintfn $"STDOUT: {toJsonText replyMessage}"

    {
        NodeId = initMessage.MessageBody.NodeId
        ClusterNodeIds = initMessage.MessageBody.AllNodeIds
    }
