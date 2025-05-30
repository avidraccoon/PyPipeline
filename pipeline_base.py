from functools import lru_cache, wraps
from typing import Any, Dict, Generic, List, Self, Type, TypeAlias, TypeVar, Callable, Union
from dataclasses import dataclass

CACHE_SIZE = None
CACHE_DEFAULT_SIZE = 128

PipelineDataType = TypeVar('PipelineDataType')

@dataclass
class PipelineDataDefinition(Generic[PipelineDataType]):
    type: Type[PipelineDataType]
    name: str

PipelineInputMap: TypeAlias = Dict[str, Type[Any]]
PipelineOutputMap: TypeAlias = Dict[str, Type[Any]]
PipelineDataMap: TypeAlias = Dict[str, Any]

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

class PipelineTransformer:
    
    def __init__(self, func):
        super().__init__()
        if not getattr(func, "_pipeline_transformer", False):
            raise ValueError("function is not a transformer must use the @transformer or @provider decorators")
        self._func: Callable = func
        self._inputs: PipelineInputMap = getattr(func, "_pipeline_inputs", {})
        self._outputs: PipelineOutputMap = getattr(func, "_pipeline_outputs", {})
        self._cached: bool = getattr(func, "_pipeline_cache", False)

    def has_cache(self):
        return self._cached

    def clear_cache(self):
        if self._cached:
            try:
                self._func.cache_clear()
            except AttributeError:
                pass

    def cache_info(self):
        if self._cached:
            try:
                return self._func.cache_info()
            except AttributeError:
                pass
        return None

    def get_inputs(self):
        return self._inputs
    
    def get_outputs(self):
        return self._outputs
    
    def _validate_inputs(self, inputs: PipelineDataMap):
        for key, expected_type in self._inputs.items():
            if key not in inputs:
                raise KeyError(f"Missing required input: {key}")
            if not isinstance(inputs[key], expected_type):
                raise TypeError(f"Expected type {expected_type} for {key}, got {type(inputs[key])}")
        
    def transform(self, inputs: PipelineDataMap) -> PipelineDataMap:
        self._validate_inputs(inputs)
        return self._func(inputs)
    
    def __repr__(self):
        return f"<{self.__class__.__name__} func={getattr(self._func, '__name__', 'anonymous')} inputs={list(self._inputs.keys())} outputs={list(self._outputs.keys())}>"


def transformer(func=None, *, inputs=None, outputs=None):
    if inputs is None: inputs = {}
    if outputs is None: outputs = {}
    @wraps(func)
    def wrapper(f):
        f._pipeline_transformer = True
        f._pipeline_inputs = inputs
        f._pipeline_outputs = outputs
        return f
    if func is None:
        return wrapper
    return wrapper(func)

    

class PipelineStage:

    def __init__(self):
        super().__init__()
        self._inputs: PipelineInputMap = {}
        self._outputs: PipelineOutputMap = {}

    def get_inputs(self):
        return self._inputs
    
    def get_outputs(self):
        return self._outputs

    def _validate_inputs(self, inputs: PipelineDataMap):
        for key, expected_type in self._inputs.items():
            if key not in inputs:
                raise KeyError(f"Missing required input: {key}")
            if not isinstance(inputs[key], expected_type):
                raise TypeError(f"Expected type {expected_type} for {key}, got {type(inputs[key])}")

    def run(self, inputs: PipelineDataMap, pipeline=None) -> PipelineDataMap:
        self._validate_inputs(inputs)

    def has_cache(self):
        return False

    def clear_cache(self):
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__} func={getattr(self._func, '__name__', 'anonymous')} inputs={list(self._inputs.keys())} outputs={list(self._outputs.keys())}>"


        

class FunctionStage(PipelineStage):

    def __init__(self, func):
        super().__init__()
        if not getattr(func, "_pipeline_stage", False):
            raise ValueError("function is not a stage must use the @stage decorator")
        self._func: Callable = func
        self._inputs: PipelineInputMap = getattr(func, "_pipeline_inputs", {})
        self._outputs: PipelineOutputMap = getattr(func, "_pipeline_outputs", {})
        self._cached: bool = getattr(func, "_pipeline_cache", False)

    def has_cache(self):
        return self._cached

    def clear_cache(self):
        if self._cached:
            try:
                self._func.cache_clear()
            except AttributeError:
                pass

    def cache_info(self):
        if self._cached:
            try:
                return self._func.cache_info()
            except AttributeError:
                pass
        return None
    
    def run(self, inputs: PipelineDataMap, pipeline=None) -> PipelineDataMap:
        self._validate_inputs(inputs)
        return self._func(inputs)

def stage(func=None, *, inputs=None, outputs=None):
    if inputs is None: inputs = {}
    if outputs is None: outputs = {}
    @wraps(func)
    def wrapper(f):
        f._pipeline_stage = True
        f._pipeline_inputs = inputs
        f._pipeline_outputs = outputs
        return f
    if func is None:
        return wrapper
    return wrapper(func)

PipelineTransformers: TypeAlias = List[PipelineTransformer]
PipelineStages: TypeAlias = List[PipelineStage]

class Pipeline:

    def __init__(self, dependencies: PipelineInputMap =None, outputs: PipelineInputMap=None):
        super().__init__()
        self.transforms: PipelineTransformers = []
        self.stages: PipelineStages = []
        self.data_records: PipelineDataMap = {}
        self._deps_set = True
        if dependencies is None:
            dependencies = {}
            self._deps_set = False
        self.dependencies: PipelineInputMap = dependencies
        self._out_set = True
        if outputs is None:
            outputs = {}
            self._out_set = False
        self.outputs: PipelineOutputMap = outputs


    def get_dependencies(self) -> PipelineInputMap:
        return self.dependencies
    
    def get_outputs(self) -> PipelineOutputMap:
        return self.outputs

    def _has_input(self, input: PipelineDataDefinition[PipelineDataType]) -> bool:
        if input.name in self.data_records:
            return True
        return any(input.name in t.get_outputs().keys() for t in self.transforms)


    def _get_input(self, input: PipelineDataDefinition[PipelineDataType]) -> PipelineDataType:
        # First, try direct data lookup
        if input.name in self.data_records:
            return self.data_records[input.name]

        # Otherwise, search for a transformer that can produce it
        for transformer in self.transforms:
            if input.name in transformer.get_outputs().keys():
                # Build input map for transformer
                required_inputs = {}
                for key, expected_type in transformer.get_inputs().items():
                    input_def = PipelineDataDefinition(type=expected_type, name=key)
                    required_inputs[key] = self.resolve_input(self, input_def)  # Recurse if needed

                result = transformer.transform(required_inputs)
                self.data_records.update(result)
                return result[input.name]  # After transform, input should be available

        raise KeyError(f"No data or transformer found for input: {input.name}")

    def resolve_input(self, parent: Self, input: PipelineDataDefinition[PipelineDataType]) -> PipelineDataType:
        if self._has_input(input):
            return self._get_input(input)
        if parent is not None:
            return parent.resolve_input(input)
        raise LookupError("Could not find way to get input")

    def _clear_data(self):
        self.data_records.clear()

    def _clear_cache(self):
        for stage in self.stages:
            stage.clear_cache()

        for transformer in self.transforms:
            transformer.clear_cache()

    def _validate_inputs(self, inputs: PipelineDataMap):
        for key, expected_type in self.dependencies.items():
            if key not in inputs:
                raise KeyError(f"Missing required input: {key}")
            if not isinstance(inputs[key], expected_type):
                raise TypeError(f"Expected type {expected_type} for {key}, got {type(inputs[key])}")

    def _run(self, inputs: PipelineDataMap, parent=None) -> PipelineDataMap:
        self._validate_inputs(inputs)
        self.data_records.update(inputs)
        result = {}
        for stage in self.stages:
            required_inputs = {}
            for key, expected_type in stage.get_inputs().items():
                input_def = PipelineDataDefinition(type=expected_type, name=key)
                try:
                    value = self.resolve_input(parent, input_def)
                    required_inputs[key] = value
                except LookupError:
                    raise KeyError(f"Missing required input '{key}' for stage {stage}")
            result = stage.run(required_inputs, self)
            self.data_records.update(result)
        return result

    def run(self, inputs: PipelineDataMap, parent=None) -> PipelineDataMap:
        self._run(inputs, parent)
        all_data = self.data_records
        return {k: v for k, v in all_data.items() if k in self.outputs.keys()}
    
    def _append_stage(self, stage: PipelineStage):
        self.stages.append(stage)
        if len(self.stages) == 1 and not self._deps_set:
            self.dependencies = stage.get_inputs()
        if not self._out_set:
            self.outputs = stage.get_outputs()

    def stage(self, stage: Union[callable, PipelineStage]) -> Self:
        if callable(stage):
            self._append_stage(FunctionStage(stage))
        else:
            self._append_stage(stage)
        return self
    
    def transformer(self, transformer: Union[callable, PipelineTransformer]) -> Self:
        if callable(transformer):
            self.transforms.append(PipelineTransformer(transformer))
        else:
            self.transforms.append(transformer)
        return self
    
    def dependency(self, dependencies: PipelineInputMap ) -> Self:
        self._deps_set = True
        self.dependencies = dependencies
        return self

    def output(self, outputs: PipelineOutputMap) -> Self:
        self._out_set = True
        self.outputs = outputs
        return self

class PipelineBranch(PipelineStage, Pipeline):

    def __init__(self):
        PipelineStage.__init__(self)
        Pipeline.__init__(self)

    def get_inputs(self):
        return Pipeline.get_dependencies(self)
    
    def get_outputs(self):
        return Pipeline.get_outputs(self)

    def run(self, inputs: PipelineDataMap, parent=None) -> PipelineDataMap:
        Pipeline._run(self, inputs, parent)
        return self.data_records