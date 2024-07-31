[<AutoOpen>]
module Types.Node

open Fleece
open FSharpPlus.Data



type NodeId = NodeId of string
with
    static member get_Codec () : Codec<'a, NodeId> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
        Codec.isomorph (fun (NodeId x) -> x) (fun x -> NodeId x) Codecs.string

type InitialNodeInfo = {
    NodeId : NodeId
    ClusterNodeIds: NonEmptySet<NodeId>
}