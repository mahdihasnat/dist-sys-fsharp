[<AutoOpen>]
module GrowOnlyCounter.Transition

open System
open FSharpPlus.Data
open Microsoft.FSharp.Core
open Types

let inline transition (node: Node) (action: Choice<Message<InputMessageBody>, unit>) : TransitionResult =
    transition {
        match action with
        | Choice2Of2 unit ->
            yield!
                node.Info.ClusterNodeIds
                |> NonEmptySet.toList
                |> List.choose (fun nodeId ->
                    if nodeId = node.Info.NodeId then None
                    else Some { Source = node.Info.NodeId; Destination = nodeId; MessageBody = Replicate node.Crdt }
                )
            return node

        | Choice1Of2 msg ->
            match msg.MessageBody with
            | InputMessageBody.Add (messageId, delta) ->

                let updatedValue =
                    node.Crdt.TryFind node.Info.NodeId |> Option.defaultValue (Value 0) |> (+) delta

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = AddAck (messageId)
                    }

                return
                    { node with
                        Crdt = node.Crdt.Add (node.Info.NodeId, updatedValue)
                    }

            | InputMessageBody.Read messageId ->
                let sum = node.Crdt.Values |> Seq.sum

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = ReadAck (messageId, sum)
                    }

                return node

            | InputMessageBody.Replicate crdt ->

                return
                    { node with
                        Crdt =
                            (node.Crdt, crdt)
                            ||> Map.fold (fun acc nodeId value -> acc.Add (nodeId, acc.TryFind nodeId |> Option.defaultValue (Value 0) |> max value))
                    }
    }
