[<AutoOpen>]
module Types.Node

open Fleece
open FSharpPlus.Data



type NodeId =
    | NodeId of string

    static member get_Codec () : Codec<'a, NodeId> when 'a :> IEncoding and 'a: (new: unit -> 'a) =
        Codec.isomorph (fun (NodeId x) -> x) (fun x -> NodeId x) Codecs.string

module NodeId =
    let SeqKv = NodeId "seq-kv"
    let LinKv = NodeId "lin-kv"

type InitialNodeInfo =
    {
        NodeId: NodeId
        ClusterNodeIds: NonEmptySet<NodeId>
    }
