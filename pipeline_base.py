from functools import wraps
from typing import Any, Dict, List, Self
from dataclasses import dataclass


class PipelineTransformer:
    pass

class PipelineProvider(PipelineTransformer):
    pass

class PipelineInputs:
    pass

class PipelineStage:

    def __init__(self):
        super.__init__()

class FunctionStage(PipelineStage):

    def __init__(self, func):
        super().__init__()
        self.func = func

    def get_inputs(self):
        return self.func._pipeline_inputs

@dataclass
class PipelineInputDefinition:
    name: str

@dataclass
class PipelineDataRecord:
    dependencies: List[str]


#dependencies
class Pipeline:

    def __init__(self):
        super.__init__()
        self.providers: List[PipelineProvider] = []
        self.stages: List[PipelineStage] = []
        self.data_records: Dict[PipelineDataRecord, Any] = {}
        self.dependencies: List[str] = []
        self.outputs: List[str] = []
        self.data: Dict[str, Any] = []

    def _has_input(self, parent):
        pass

    def resolve_input(self, parent: Self, input: str):
        if self._has_input():
            pass
        if parent is not None:
            return parent.resolve_input(input)
        raise LookupError("Could not find way to get input")

    def _clear_data(self):
        self.data.clear()

    def _clear_cache(self):
        pass

    def run(self, inputs):
        pass


class PipelineBranch(PipelineStage, Pipeline):

    def __init__(self):
        super.__init__()