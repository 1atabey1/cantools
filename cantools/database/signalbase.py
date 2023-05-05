import logging
from typing import List, Optional, Tuple, Union

from ..typechecking import ByteOrder, Choices, Interval

logger = logging.getLogger(__name__)


class SignalBase:
    def __init__(
        self,
        name: str,
        start: int,
        length: int,
        byte_order: ByteOrder = "little_endian",
        is_signed: bool = False,
        scale: Union[List[float], float] = 1.0,
        offset: Union[List[float], float] = 0.0,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        unit: Optional[str] = None,
        choices: Optional[Choices] = None,
        is_float: bool = False,
        segment_boundaries: Optional[List[Interval]] = None,
    ) -> None:
        # avoid using properties to improve encoding/decoding performance

        #: The signal name as a string.
        self.name: str = name

        #: The scale factor of the data value.
        #: For piecewise linear data each list element
        #: represents the scale for the respective segment
        self._scale: Union[List[float], float] = scale

        #: The offset of the data value.
        #: For piecewise linear data each list element
        #: represents the offset for the respective segment
        self._offset: Union[List[float], float] = offset

        #: ``True`` if the signal is a float, ``False`` otherwise.
        self.is_float: bool = is_float

        #: The minimum value of the signal, or ``None`` if unavailable.
        self.minimum: Optional[float] = minimum

        #: The maximum value of the signal, or ``None`` if unavailable.
        self.maximum: Optional[float] = maximum

        #: "A dictionary mapping signal values to enumerated choices, or
        #: ``None`` if unavailable.
        self.choices: Optional[Choices] = choices

        #: The start bit position of the signal within its message.
        self.start: int = start

        #: The length of the signal in bits.
        self.length: int = length

        #: Signal byte order as ``'little_endian'`` or ``'big_endian'``.
        self.byte_order: ByteOrder = byte_order

        #: ``True`` if the signal is signed, ``False`` otherwise. Ignore this
        #: attribute if :data:`~cantools.db.Signal.is_float` is
        #: ``True``.
        self.is_signed: bool = is_signed

        #: The unit of the signal as a string, or ``None`` if unavailable.
        self.unit: Optional[str] = unit

        #: Stores the raw start and end points of piecewise linear segments
        #: empty for all other types
        self.segment_boundaries_raw: List[Interval] = []

        #: Stores the scaled start and end points of piecewise linear segments
        #: empty for all other types
        self.segment_boundaries_scaled: List[Interval] = []
        if segment_boundaries is not None:
            if not isinstance(scale, list) or not isinstance(offset, list):
                raise ValueError(
                    "Params scale and offset need to be of type list "
                    "if segment boundaries are defined."
                )

            self._initialize_segment_boundaries(segment_boundaries)

    def _initialize_segment_boundaries(
        self, segment_boundaries: List[Interval]
    ) -> None:
        def convert(v, o, f):
            return v * f + o

        self.segment_boundaries: List[Interval] = []
        last_phys_max = None
        for i, segment in enumerate(segment_boundaries):
            self.segment_boundaries_raw.append(segment)
            start, end = segment
            scaled_segment = (
                convert(start, self._offset[i], self._scale[i]),  # type: ignore
                convert(end, self._offset[i], self._scale[i]),  # type: ignore
            )
            self.segment_boundaries_scaled.append(scaled_segment)
            if last_phys_max is None:
                last_phys_max = scaled_segment[1]
                continue

            if last_phys_max >= scaled_segment[0]:
                logger.warning(
                    f"Piecewise linear type: {self.name} has overlapping segments! "
                    f"Segment {i} starts at phys val {scaled_segment[0]} "
                    f"but one of the prev segments ended at {last_phys_max}. "
                    "Encoding might be ambiguous."
                )

            last_phys_max = max(scaled_segment[1], last_phys_max)

    def choice_string_to_number(self, string: str) -> int:
        if self.choices is None:
            raise ValueError(f"Signal {self.name} has no choices.")

        for choice_number, choice_value in self.choices.items():
            if str(choice_value) == str(string):
                return choice_number

        raise KeyError(f"Choice {string} not found in data element {self.name}.")

    def _get_offset_scaling_from_list(
        self, val: float, segments: List[Interval]
    ) -> Tuple[float, float]:
        assert not isinstance(self._offset, float) and not isinstance(self._scale, float)

        for i, segment in enumerate(segments):
            start, end = segment
            if start <= val <= end:
                return self._offset[i], self._scale[i]
        else:
            err_text = [f"{start} <= x <= {end}" for start, end in segments]
            raise ValueError(
                f"Value {val} is not in ranges: \n {' OR '.join(err_text)}"
            )

    def get_offset_scaling_from_raw(
        self, raw_val: Optional[Union[int, float]] = None
    ) -> Tuple[float, float]:
        """Get the applicable offset and scaling for the given raw value.

        If data type only defines one set of offset/scaling then
        the `raw_val` param can be omitted
        """
        if raw_val is None or not self.segment_boundaries_raw:
            try:
                return self._offset[0], self._scale[0]  # type: ignore
            except TypeError:
                return self.offset, self.scale

        return self._get_offset_scaling_from_list(raw_val, self.segment_boundaries_raw)

    def get_offset_scaling_from_scaled(
        self, scaled_val: Optional[float] = None
    ) -> Tuple[float, float]:
        """Get the applicable offset and scaling for the given scaled value.

        If data type only defines one set of offset/scaling then
        the `scaled_val` param can be omitted
        """
        if scaled_val is None or not self.segment_boundaries_scaled:
            try:
                return self._offset[0], self._scale[0]  # type: ignore
            except TypeError:
                return self.offset, self.scale

        return self._get_offset_scaling_from_list(
            scaled_val, self.segment_boundaries_scaled
        )

    @property
    def offset(self) -> float:
        """Return first or only offset element"""
        if isinstance(self._offset, list):
            return self._offset[0]
        else:
            return self._offset

    @offset.setter
    def offset(self, new_offset: Union[float, List[float]]) -> None:
        """Set offset"""
        self._offset = new_offset

    @property
    def scale(self) -> float:
        """Return first or only scale element"""
        if isinstance(self._scale, list):
            return self._scale[0]
        else:
            return self._scale

    @scale.setter
    def scale(self, new_scale: Union[float, List[float]]) -> None:
        """Set scale"""
        self._scale = new_scale
