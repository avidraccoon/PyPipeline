from typing import Dict
from pipeline_base import stage, Pipeline, transformer

@transformer(output_name="x2")
def double(x: int) -> int:
    return x * 2

@stage(output_name="x2plus1")
def add_one(x2: int) -> int:
    return x2 + 1

main = (
    Pipeline()
        .dependency({"x": int})
        .transformer(double)
        .stage(add_one)
)

print(main.run({"x": 5}))  # {'x2plus1': 11}
