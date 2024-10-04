[<AutoOpen>]
module Types.Transition

open FSharpPlus.Math

type GenericTransitionResult<'Node, 'OutputMessage> = Accumulator<'Node, 'OutputMessage>
type FutureTransition<'Node, 'OutputMessage, 'T> = ('Node * 'T -> GenericTransitionResult<'Node, 'OutputMessage>) -> GenericTransitionResult<'Node, 'OutputMessage>

type TransitionBuilder() =
    inherit AccumulatorBuilder()

    member this.Bind(x: FutureTransition<'Node, 'OutputMessage, 'T>, f: 'Node * 'T -> GenericTransitionResult<'Node, 'OutputMessage>) : GenericTransitionResult<'Node, 'OutputMessage> = x f

let transition = new TransitionBuilder()