from pipeline_base import stage, Pipeline, transformer


@transformer(inputs={"x": int}, outputs={"x2": int})
def double(inputs):
    return {"x2": inputs["x"] * 2}

@stage(inputs={"x2": int}, outputs={"x2plus1": int})
def add_one(inputs):
    return {"x2plus1": inputs["x2"] + 1}

main = (
    Pipeline()
        .dependency({"x": int})
        .transformer(double)
        .stage(add_one)
)

print(main.run({"x": 5})) # Expected Output: {'x2plus1': 11}
