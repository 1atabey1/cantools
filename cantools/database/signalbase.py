import contextlib
from typing import List, Optional, Tuple, Union

from ..typechecking import ByteOrder, Choices, Interval, SignalValueType
from .namedsignalvalue import NamedSignalValue


class SignalBase:
    def __init__(
        self,
        name: str,
        start: int,
        length: int,
        byte_order: ByteOrder = "little_endian",
        is_signed: bool = False,
        scale: Union[List[float], int, float] = 1,
        offset: Union[List[float], int, float] = 0,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        unit: Optional[str] = None,
        choices: Optional[Choices] = None,
        is_float: bool = False,
        segment_intervals_raw: Optional[List[Interval]] = None,
    ) -> None:
        # avoid using properties to improve encoding/decoding performance

        # ensure that either the parameters for a globally-scaled
        # linear function are given or the ones for a piecewise linear
        # one. TODO: using a piecewise linear function with one
        # segment to represent globally scaled conversion functions
        # would probably simplify stuff here
        if isinstance(scale, list) or \
           isinstance(offset, list) or \
           isinstance(segment_intervals_raw, list):
            assert isinstance(scale, list)
            assert isinstance(offset, list)
            if not isinstance(segment_intervals_raw, list):
                print("foo")
            assert isinstance(segment_intervals_raw, list)
            assert len(segment_intervals_raw) == len(scale)
            assert len(segment_intervals_raw) == len(offset)

        #: The signal name as a string.
        self.name: str = name

        #: The scaling factor of the data value.
        #:
        #: For piecewise linear data, each list element
        #: represents the scaling factor for the respective segment
        self._scale: Union[List[float], float] = scale

        #: The offset of the data value.
        #:
        #: For piecewise linear data each list element
        #: represents the offset for the respective segment
        self._offset: Union[List[float], float] = offset

        #: ``True`` iff ``float`` ought to be used for the internal
        #: representation of signal
        self.is_float: bool = is_float

        #: The minimum value of the signal, or ``None`` if unspecified.
        self.minimum: Optional[float] = minimum

        #: The maximum value of the signal, or ``None`` if unspecified.
        self.maximum: Optional[float] = maximum

        #: "A dictionary mapping signal values to enumerated choices, or
        #: ``None`` if unspecified.
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

        #: The raw values the of start and end points of piecewise
        #: linear segments.  Empty if the conversion function is not
        #: piecewise linear.
        self.segment_intervals_raw: List[Interval] = \
            [] if segment_intervals_raw is None else segment_intervals_raw

        #: The scaled values of the start and end points of piecewise
        #: linear segments. Empty if the conversion function is not
        #: piecewise linear.
        self.segment_intervals_scaled: List[Interval] = []
        if segment_intervals_raw is not None:
            assert isinstance(scale, list)
            assert isinstance(offset, list)
            convert = lambda value, factor, offset: value*factor + offset
            for raw_interval, factor, delta in zip(segment_intervals_raw,
                                                   scale,
                                                   offset):
                x0, x1 = raw_interval
                self.segment_intervals_scaled.append(
                    (convert(x0, factor, delta),
                     convert(x1, factor, delta),)
                )

    def choice_string_to_number(self, string: Union[str, NamedSignalValue]) -> int:
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
        if raw_val is None or not self.segment_intervals_raw:
            try:
                return self._offset[0], self._scale[0]  # type: ignore
            except TypeError:
                return self.offset, self.scale

        return self._get_offset_scaling_from_list(raw_val, self.segment_intervals_raw)

    def get_offset_scaling_from_scaled(
        self, scaled_val: Optional[float] = None
    ) -> Tuple[float, float]:
        """Get the applicable offset and scaling for the given scaled value.

        If data type only defines one set of offset/scaling then
        the `scaled_val` param can be omitted
        """
        if scaled_val is None or not self.segment_intervals_scaled:
            try:
                return self._offset[0], self._scale[0]  # type: ignore
            except TypeError:
                return self.offset, self.scale

        return self._get_offset_scaling_from_list(
            scaled_val, self.segment_intervals_scaled
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

    def raw_to_scaled(
        self, raw: Union[int, float], decode_choices: bool = True
    ) -> SignalValueType:
        """Convert an internal raw value according to the defined scaling or value table.

        :param raw:
            The raw value
        :param decode_choices:
            If `decode_choices` is ``False`` scaled values are not
            converted to choice strings (if available).
        :return:
            The calculated scaled value
        """
        if decode_choices:
            with contextlib.suppress(KeyError, TypeError):
                return self.choices[raw]  # type: ignore[index]

        if self.offset == 0 and self.scale == 1:
            # treat special case to avoid introduction of unnecessary rounding error
            return raw
        return raw * self.scale + self.offset

    def scaled_to_raw(self, scaled: SignalValueType) -> Union[int, float]:
        """Convert a scaled value to the internal raw value.

        :param scaled:
            The scaled value.
        :return:
            The internal raw value.
        """
        if isinstance(scaled, (float, int)):
            _transform = float if self.is_float else round
            if self.offset == 0 and self.scale == 1:
                # treat special case to avoid introduction of unnecessary rounding error
                return _transform(scaled)  # type: ignore[operator,no-any-return]

            return _transform((scaled - self.offset) / self.scale)  # type: ignore[operator,no-any-return]

        if isinstance(scaled, (str, NamedSignalValue)):
            return self.choice_string_to_number(str(scaled))

        raise TypeError(f"Conversion of type {type(scaled)} is not supported.")
