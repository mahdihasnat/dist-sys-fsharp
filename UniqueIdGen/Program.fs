[<AutoOpen>]
module UniqueIdGen.Program

open UniqueIdGen
open System
open Types
open Utilities
open Fleece.SystemTextJson
let generateId (node: Node) : string * Node =
    let (NodeId (nodeId:string)) = node.Info.NodeId
    let newId = $"{nodeId}-{node.Counter}"
    newId, { node with Counter = node.Counter + 1 }

let rec processStdin (node: Node) : unit =
    let str = Console.ReadLine ()
    if str = null then
        ()
    else
        eprintfn $"STDIN: {str}"
        let gen_message =
            match ofJsonText<Message<GenerateMessageBody>> str with
            | Error e ->
                failwithf $"{e}"
            | Ok message ->
                message
        let (id, node) = generateId node
        let replyMessageBody : GenerateReplyMessageBody =
            {
                InReplyTo = gen_message.MessageBody.MessageId
                Id = id
            }
        let replyMessage : Message<GenerateReplyMessageBody> =
            {
                Source = node.Info.NodeId
                Destination = gen_message.Source
                MessageBody = replyMessageBody
            }
        printfn $"{toJsonText replyMessage}"
        eprintfn $"STDOUT: {toJsonText replyMessage}"
        processStdin node

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()

    processStdin {
        Counter = 0
        Info = nodeInfo
    }
    0