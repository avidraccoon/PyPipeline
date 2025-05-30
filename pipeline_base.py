from functools import lru_cache, wraps
from typing import Any, Dict, Generic, List, Self, Type, TypeAlias, TypeVar
from dataclasses import dataclass

CACHE_SIZE = None
CACHE_DEFAULT_SIZE = 128

PipelineDataType = TypeVar('PipelineDataType')

@dataclass
class PipelineDataDefinition(Generic[PipelineDataType]):
    type: Type[PipelineDataType]
    value: PipelineDataType

PipelineInputMap: TypeAlias = Dict[str, Type[Any]]
PipelineOutputMap: TypeAlias = Dict[str, Type[Any]]
PipelineDataMap: TypeAlias = Dict[str, PipelineDataDefinition[Any]]

PipelineStageInputs = TypeVar('PipelineStageInputs', PipelineInputMap)
PipelineStageOutputs = TypeVar('PipelineStageOutputs', PipelineOutputMap)

def cache(func=None, *, size=CACHE_DEFAULT_SIZE):
    if func is None:
        # Decorator called with parentheses and optional size, e.g. @cache(size=5)
        def wrapper(f):
            f._pipeline_cache = True
            return lru_cache(maxsize=size)(f)
        return wrapper
    else:
        func._pipeline_cache = True
        # Decorator called without parentheses, e.g. @cache
        return lru_cache(maxsize=size)(func)

class PipelineTransformer():
    
    def __init__(self, func):
        super().__init__()
        if getattr(func, "_pipeline_transformer", False):
            raise ValueError("function is not a transformer must use the @transformer or @provider decorators")
        self.func = func
        self.inputs = getattr(func, "_pipeline_inputs", {})
        self.outputs = getattr(func, "_pipeline_inputs", {})
        self._cached = getattr(func, "_pipeline_cache", False)

    def has_cache(self):
        self._cached

    def clear_cache(self):
        if self._cached:
            self.func.cache_clear()

    #TODO: Maybe add input type verification
        
    def transform(self, inputs: PipelineDataMap) -> PipelineDataMap:
        return self.func(inputs)

def transformer(func=None, *, inputs: PipelineInputMap, outputs: PipelineOutputMap):
    @wraps(func)

    

class PipelineStage(Generic[PipelineStageInputs, PipelineStageOutputs]):

    def __init__(self):
        super().__init__()

class FunctionStage(PipelineStage[PipelineStageInputs, PipelineStageOutputs]):

    def __init__(self, func):
        super().__init__()
        if getattr(func, "_pipeline_stage", False):
            raise ValueError("function is not a stage must use the @stage decorator")
        self.func = func

    def get_inputs(self):
        return self.func._pipeline_inputs


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
        self.data_records.clear()

    def _clear_cache(self):
        pass

    def run(self, inputs: Dict[str, Type]):
        pass


class PipelineBranch(PipelineStage[PipelineStageInputs, PipelineStageOutputs], Pipeline):

    def __init__(self):
        PipelineStage[PipelineStageInputs, PipelineStageOutputs].__init__(self)
        Pipeline.__init__(self)