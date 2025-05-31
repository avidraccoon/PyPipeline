import dataclasses
from functools import lru_cache, wraps
from types import NoneType
from typing import Annotated, Any, Dict, Generic, List, NamedTuple, Self, Tuple, Type, TypeAlias, TypeVar, Callable, Union, get_args, get_origin, get_type_hints, _GenericAlias
from dataclasses import dataclass
import inspect

CACHE_SIZE = None
CACHE_DEFAULT_SIZE = 128
THROW_ERROR_ON_MISSING_RET_ANN = False

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


def normalize_io(io):
    if io is None:
        return {}
    if isinstance(io, dict):
        return io
    if isinstance(io, list):
        return {k: Any for k in io}
    if isinstance(io, str):
        return {io: Any}
    raise TypeError("inputs/outputs must be dict, list, or str")

def infer_input_types(func):
    sig = inspect.signature(func)
    inputs = {}
    for name, param in sig.parameters.items():
        if name == 'self':  # ignore methods self param
            continue
        if param.annotation is inspect.Parameter.empty:
            inputs[name] = Any
        else:
            inputs[name] = param.annotation
    return inputs

def is_namedtuple_instance(x):
    return (
        isinstance(x, tuple) and
        hasattr(x, "_fields")
    )

def normalize_result(result, output_names, stage_name):
    if isinstance(result, dict):
        return result

    if isinstance(result, (tuple, list)):
        if len(result) != len(output_names):
            raise ValueError(
                f"{stage_name} returned {len(result)} values, "
                f"but expected {len(output_names)}: {output_names}"
            )
        return dict(zip(output_names, result))

    if is_namedtuple_instance(result):
        return result._asdict()

    if dataclasses.is_dataclass(result):
        return dataclasses.asdict(result)

    if len(output_names) == 1:
        return {output_names[0]: result}
    
    if len(output_names) == 0:
        return {}

    raise TypeError(
        f"{stage_name} returned a single value, but multiple outputs are expected: {output_names}"
    )

def infer_output_types(func, name: Union[str, None] = None, names: Union[List[str], None] = None) -> Dict[str, type]:
    hints = get_type_hints(func)
    ret_ann = hints.get('return', None)
    if ret_ann is None:
        if THROW_ERROR_ON_MISSING_RET_ANN:
            raise SyntaxError("Must use return type annotation")
        return {}

    origin = get_origin(ret_ann)
    args = get_args(ret_ann)

    # Case: Dict[str, T] – can't infer names
    if origin in (dict, Dict):
        return {}

    # Case: Tuple[T1, T2, ...]
    if origin in (tuple, Tuple):
        if names and len(names) == len(args):
            return {n: t for n, t in zip(names, args)}
        return {}

    # Case: TypedDict
    if isinstance(ret_ann, type) and issubclass(ret_ann, dict) and hasattr(ret_ann, '__annotations__'):
        return dict(ret_ann.__annotations__)

    # Case: Annotated[Dict, {"x": int, ...}]
    if origin is Annotated and isinstance(args[1], dict):
        return args[1]

    if ret_ann is NoneType:
        return {}
    

    # Case: single primitive type (int, float, bool, str, etc.)
    if isinstance(ret_ann, type):
        key = name if name else "output"
        return {key: ret_ann}

    return {}

class PipelineTransformer:
    
    def __init__(self, func):
        super().__init__()
        if not getattr(func, "_pipeline_transformer", False):
            raise ValueError("function is not a transformer must use the @transformer or @provider decorators")
        self._func: Callable = func
        self._inputs: PipelineInputMap = getattr(func, "_pipeline_inputs", {})
        self._outputs: PipelineOutputMap = getattr(func, "_pipeline_outputs", {})
        self._unwrap_inputs: bool = getattr(func, "_pipeline_unwrap_inputs", False)
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
        if self._unwrap_inputs:
            result = self._func(**inputs)
        else:
            result = self._func(inputs)
        # Wrap the output if it's not a dict
        output_names = list(self._outputs)
        result = normalize_result(result, output_names, self._get_name())
        return result
    
    def _get_name(self):
        return getattr(self._func, '__name__', 'anonymous')
    
    def __repr__(self):
        return f"<{self.__class__.__name__} func={self._get_name()} inputs={list(self._inputs.keys())} outputs={list(self._outputs.keys())}>"


def transformer(func=None, *, inputs=None, outputs=None, output_name=None, output_names=None):
    def decorator(f):
        f._pipeline_transformer = True
        f._pipeline_inputs = normalize_io(inputs) if inputs is not None else infer_input_types(f)
        f._pipeline_outputs = normalize_io(outputs) if outputs is not None else infer_output_types(f, name=output_name, names=output_names)
        f._pipeline_unwrap_inputs = inputs is None
        return f
    
    if func is None:
        return decorator
    return decorator(func)

    

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

    def _get_name(self):
        return 'anonymous'


    def __repr__(self):
        return f"<{self.__class__.__name__} func={self._get_name()} inputs={list(self._inputs.keys())} outputs={list(self._outputs.keys())}>"


        

class FunctionStage(PipelineStage):

    def __init__(self, func):
        super().__init__()
        if not getattr(func, "_pipeline_stage", False):
            raise ValueError("function is not a stage must use the @stage decorator")
        self._func: Callable = func
        self._inputs: PipelineInputMap = getattr(func, "_pipeline_inputs", {})
        self._outputs: PipelineOutputMap = getattr(func, "_pipeline_outputs", {})
        self._unwrap_inputs: bool = getattr(func, "_pipeline_unwrap_inputs", False)
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
    
    def _get_name(self):
        return getattr(self._func, '__name__', 'anonymous')

    def run(self, inputs: PipelineDataMap, pipeline=None) -> PipelineDataMap:
        self._validate_inputs(inputs)
        if self._unwrap_inputs:
            result = self._func(**inputs)
        else:
            result = self._func(inputs)
        # Wrap the output if it's not a dict
        output_names = list(self._outputs)
        result = normalize_result(result, output_names, self._get_name())
        return result
    

def stage(func=None, *, inputs=None, outputs=None, output_name=None, output_names=None):
    def decorator(f):
        f._pipeline_stage = True
        f._pipeline_inputs = normalize_io(inputs) if inputs is not None else infer_input_types(f)
        f._pipeline_outputs = normalize_io(outputs) if outputs is not None else infer_output_types(f, name=output_name, names=output_names)
        f._pipeline_unwrap_inputs = inputs is None
        return f

    if func is None:
        return decorator
    return decorator(func)

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
        
class MatchCaseBranch(PipelineBranch):
    def __init__(self, key_name: str):
        super().__init__()
        self.key_name = key_name
        self.cases: List[Tuple[Any, PipelineBranch]] = []
        self.default_branch: PipelineBranch | None = None
        self.finally_branch: PipelineBranch | None = None

    def case(self, value):
        branch = PipelineBranch()
        self.cases.append((value, branch))
        return branch

    def default(self):
        self.default_branch = PipelineBranch()
        return self.default_branch

    def finally_(self):
        self.finally_branch = PipelineBranch()
        return self.finally_branch

    def get_inputs(self):
        # Inputs needed include the match key plus inputs required by any case/default/finally
        inputs = {self.key_name: Any}
        for _, branch in self.cases:
            inputs.update(branch.get_inputs())
        if self.default_branch:
            inputs.update(self.default_branch.get_inputs())
        if self.finally_branch:
            inputs.update(self.finally_branch.get_inputs())
        return inputs

    def get_outputs(self):
        # Outputs are union of all branches outputs
        outputs = {}
        for _, branch in self.cases:
            outputs.update(branch.get_outputs())
        if self.default_branch:
            outputs.update(self.default_branch.get_outputs())
        if self.finally_branch:
            outputs.update(self.finally_branch.get_outputs())
        return outputs

    def run(self, inputs: PipelineDataMap, parent=None) -> PipelineDataMap:
        match_value = inputs.get(self.key_name)
        result = {}

        # Run matched case
        matched = False
        for value, branch in self.cases:
            if value == match_value:
                case_result = branch.run(inputs, parent)
                result.update(case_result)
                matched = True
                break
        
        # Run default if no case matched
        if not matched and self.default_branch:
            default_result = self.default_branch.run(inputs, parent)
            result.update(default_result)

        # Always run finally
        if self.finally_branch:
            finally_result = self.finally_branch.run(inputs, parent)
            result.update(finally_result)

        return result


class MatchHelper:
    def __init__(self, pipeline: Pipeline, key_name: str):
        self.pipeline = pipeline
        self.branch = MatchCaseBranch(key_name)

    def case(self, value):
        return self.branch.case(value)

    def default(self):
        return self.branch.default()

    def finally_(self):
        return self.branch.finally_()

    def end(self):
        self.pipeline.stage(self.branch)
        return self.pipeline


def match(self, key_name: str, fn: Callable[[MatchHelper], None]):
    helper = MatchHelper(self, key_name)
    fn(helper)
    return helper.end()


# Add match method to Pipeline dynamically
setattr(Pipeline, "match", match)
