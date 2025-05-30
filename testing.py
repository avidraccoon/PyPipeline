from pipeline_base import stage, Pipeline, PipelineBranch, FunctionStage


@stage(inputs={"x": int}, outputs={"x2": int})
def double(inputs):
    return {"x2": inputs["x"] * 2}

@stage(inputs={"x2": int}, outputs={"x2plus1": int})
def add_one(inputs):
    return {"x2plus1": inputs["x2"] + 1}

main = Pipeline()
main.dependencies = {"x": int}
main.outputs = {"x2plus1": int}

branch = PipelineBranch()
branch.dependencies = {"x": int}
branch.outputs = {"x2": int}
branch.stages.append(FunctionStage(double))

main.stages.append(branch)
main.stages.append(FunctionStage(add_one))

print(main.run({"x": 5}))  # Expected output: {'x2plus1': 11}
