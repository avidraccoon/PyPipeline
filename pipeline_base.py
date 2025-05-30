from functools import wraps
from typing import Any, Dict, Generic, List, Self, Type, TypeAlias, TypeVar
from dataclasses import dataclass


class PipelineTransformer:
    pass

class PipelineProvider(PipelineTransformer):
    pass

class PipelineInputs:
    pass

class PipelineStage:

    def __init__(self):
        super().__init__()

class FunctionStage(PipelineStage):

    def __init__(self, func):
        super().__init__()
        self.func = func

    def get_inputs(self):
        return self.func._pipeline_inputs





@dataclass
class PipelineDataRecord:
    dependencies: List[str]

PipelineDataType = TypeVar('PipelineDataType')

@dataclass
class PipelineDataDefinition(Generic[PipelineDataType]):
    type: Type[PipelineDataType]
    value: PipelineDataType

PipelineInputMap: TypeAlias = Dict[str, Type[Any]]
PipelineOutputMap: TypeAlias = Dict[str, Type[Any]]
PipelineDataMap: TypeAlias = Dict[str, PipelineDataDefinition[Any]]
PipelineTransformers: TypeAlias = List[PipelineTransformer[PipelineInputMap, PipelineOutputMap]]
PipelineStages: TypeAlias = List[PipelineStage[PipelineInputMap, PipelineOutputMap]]

class Pipeline:

    def __init__(self):
        super().__init__()
        self.transforms: PipelineTransformers = []
        self.stages: PipelineStages = []
        self.data_records: PipelineDataMap = {}
        self.dependencies: PipelineInputMap = {}
        self.outputs: PipelineOutputMap = {}

    def _has_input(self, input: PipelineDataDefinition[PipelineDataType]) -> bool:
        pass

    def _get_input(self, input: PipelineDataDefinition[PipelineDataType]) -> PipelineDataType:
        pass

    def resolve_input(self, parent: Self, input: PipelineDataDefinition[PipelineDataType]) -> PipelineDataType:
        if self._has_input(input):
            pass
        if parent is not None:
            return parent.resolve_input(input)
        raise LookupError("Could not find way to get input")

    def _clear_data(self):
        self.data.clear()

    def _clear_cache(self):
        pass

    def run(self, inputs: Dict[str, Type]):
        pass


class PipelineBranch(PipelineStage, Pipeline):

    def __init__(self):
        PipelineStage.__init__(self)
        Pipeline.__init__(self)