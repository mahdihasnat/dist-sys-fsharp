[<AutoOpen>]
module Types.Node

open Fleece
open FSharpPlus.Data


let QQQ<'T> = failwithf "wow"

type NodeId = NodeId of string
with
    static member get_Codec () =
        Codec.create (fun x -> Ok(NodeId x)) (fun (NodeId x) -> x)
        |> Codec.compose (Codecs.string)

type InitialNodeInfo = {
    NodeId : NodeId
    ClusterNodeIds: NonEmptyList<NodeId>
}