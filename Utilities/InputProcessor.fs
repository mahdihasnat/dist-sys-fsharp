[<AutoOpen>]
module Utilities.InputProcessor

open System
open System.Threading
open Microsoft.FSharp.Control
open System.Threading.Tasks
open Fleece.SystemTextJson
open Types


let inline processStdin
    ((nodeRef: ref<'Node>), semaphore: SemaphoreSlim)
    (transition: 'Node -> Choice<'InputAction, 'TimerAction> -> 'Node * List<Message< ^OutputMessage >>)
    : Task<unit> =

    task {
        let mutable continueLoop = true

        while continueLoop do
            let! str = Task.Run (fun _ -> Console.ReadLine ())

            if str = null then
                continueLoop <- false
            else
                eprintfn $"STDIN: {str}"

                let msg =
                    match ofJsonText<'InputAction> str with
                    | Error e -> failwithf $"Failed to parse input message: {e}"
                    | Ok msg -> msg

                do! semaphore.WaitAsync ()
                let (node', messages) = transition nodeRef.Value (Choice1Of2 msg)
                nodeRef.Value <- node'

                messages
                |> List.iter (fun msg ->
                    let json = toJsonText msg
                    printfn $"{json}"
                    eprintfn $"STDOUT: {json}")

                semaphore.Release () |> ignore

        return ()
    }
