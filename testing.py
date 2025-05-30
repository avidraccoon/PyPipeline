from pipeline_base import stage, Pipeline, transformer

@transformer(output_name="x2")
def double(x: int) -> int:
    return x * 2

@stage(output_name="x2plus1")
def add_one(x2: int) -> int:
    return x2 + 1

@stage
def display(x2plus1: int) -> None:
    print(x2plus1)

@stage
def display_debug(x2plus1: int) -> None:
    print("Hello", x2plus1)

main = (
    Pipeline()
        .dependency({"x": int})
        .transformer(double)
        .stage(add_one)
        .match("x2plus1", lambda m: (
            m.case(11).stage(display_debug),
            m.default().stage(display),
            m.finally_().stage(lambda inputs: print("Always runs."))
        ))
)

print(main.run({"x": 5}))  # {'x2plus1': 11}