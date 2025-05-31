from pipeline_base import stage, Pipeline, transformer

@transformer(output_name="x2")
def double(x: int) -> int:
    return x * 2

@stage(output_name="x2plus1")
def add_one(x2: int) -> int:
    return x2 + 1

@stage
def display_success() -> None:
    print("It worked")

@stage
def display_failure() -> None:
    print("It failed")

@stage
def display_values(x: int, x2:int, x2plus1: int) -> None:
    print(f"x: {x}, x2: {x2}, x2plus1: {x2plus1}")

main = (
    Pipeline()
        .dependency({"x": int})
        .transformer(double)
        .stage(add_one)
        .match("x2plus1", lambda m: (
            m.case(11).stage(display_success),
            m.default().stage(display_failure),
            m.finally_().stage(display_values)
        ))
)

print(main.run({"x": 5}))  # {'x2plus1': 11}