[<AutoOpen>]
module Utilities.TopologyGenerator

open Types
open FSharpPlus
open FSharpPlus.Data
open System

let generateGraph (nodes: NonEmptySet<NodeId>) : Map<NodeId, Set<NodeId>> =
    let nodes = nodes |> NonEmptySet.toSeq
    let n = nodes |> Seq.length
    let k = Math.Sqrt (float n) |> int
    let blocks = nodes |> Seq.chunkBySize k

    let edgesWithinBlock: seq<NodeId * NodeId> =
        blocks |> Seq.collect (fun nodes -> Seq.allPairs nodes nodes)

    let edgesBetweenBlocks: seq<NodeId * NodeId> =
        blocks
        |> Seq.collect (Seq.indexed)
        |> Seq.groupBy fst
        |> Seq.map snd
        |> Seq.map (Seq.map snd)
        |> Seq.collect (fun nodes -> Seq.allPairs nodes nodes)

    Seq.append edgesWithinBlock edgesBetweenBlocks
    |> Seq.groupBy fst
    |> Seq.map (fun (src, dests) -> src, dests |> Seq.map snd |> Set.ofSeq |> Set.remove src)
    |> Map.ofSeq
