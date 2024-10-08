[<AutoOpen>]
module Types.Accumulator


type Accumulator<'T, 'SideEffect> = 'T * List<'SideEffect>

type AccumulatorBuilder () =
    member this.Return (x: 'T) : Accumulator<'T, 'SideEffect> = x, []
    member this.Yield (x: 'SideEffect) : Accumulator<unit, 'SideEffect> = (), [ x ]

    member this.Combine ((x: unit, sideEffects1), (y, sideEffects2)) : Accumulator<'T, 'SideEffect> = y, sideEffects1 @ sideEffects2

    member this.Delay (f: unit -> Accumulator<'T, 'SideEffect>) : Accumulator<'T, 'SideEffect> = f ()
    member this.ReturnFrom (x: Accumulator<'T, 'SideEffect>) = x

    member this.Bind (x: Accumulator<'T, 'SideEffect>, f: 'T -> Accumulator<'U, 'SideEffect>) : Accumulator<'U, 'SideEffect> =
        let (value, sideEffects1) = x
        let (newValue, sideEffects2) = f value
        newValue, sideEffects1 @ sideEffects2

let accumulator = new AccumulatorBuilder ()
