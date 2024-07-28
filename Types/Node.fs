[<AutoOpen>]
module Types.Node

open Fleece
open FSharpPlus.Data

type NodeId = NodeId of string
with
    static member get_Codec () : Codec<_, NodeId> =
        Codec.create (fun x -> Ok(NodeId x)) (fun (NodeId x) -> x)
        |> Codec.compose (Codecs.string)

type InitialNodeInfo = {
    NodeId : NodeId
    ClusterNodeIds: NonEmptyList<NodeId>
}