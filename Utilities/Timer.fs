[<AutoOpen>]
module Utilities.Timer

open System
open System.Threading
open Microsoft.FSharp.Control
open System.Threading.Tasks
open Fleece.SystemTextJson
open Types

let inline repeatSchedule
        (timerInterval: TimeSpan)
        (nodeRef: ref<'Node>, semaphore: SemaphoreSlim)
        (generator: 'Node -> 'TimerAction)
        (transition: 'Node -> Choice<'InputAction, 'TimerAction> -> 'Node * List<Message<^OutputMessage>>)
            : Task<unit> =
    let mutable lastAction = DateTimeOffset.Now
    task {
        while true do
            let now = DateTimeOffset.Now
            if now < lastAction + timerInterval then
                do! Task.Delay (lastAction + timerInterval - now)
            lastAction <- DateTimeOffset.Now

            do! semaphore.WaitAsync ()

            let action = generator nodeRef.Value
            let (node', messages) = transition nodeRef.Value (Choice2Of2 action)
            nodeRef.Value <- node'
            messages
            |> List.iter (fun msg ->
                let json = toJsonText msg
                printfn $"{json}"
                eprintfn $"STDOUT: {json}"
            )
            semaphore.Release () |> ignore
    }