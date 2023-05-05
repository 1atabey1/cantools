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

    def get_offset_and_scaling_for_raw(self, raw_val: float) -> Tuple[float, float]:
        """Return the applicable offset and scaling factor for a given
        raw value.
        """
        if isinstance(self._offset, (int, float)):
            assert isinstance(self._scale, (int, float))
            # global linear scaling
            return self._offset, self._scale

        assert isinstance(self._scale, list)
        for interval, offset, scale in zip(self.segment_intervals_raw,
                                           self._offset,
                                           self._scale,):
            start, end = interval
            if start <= raw_val <= end:
                return offset, scale
        else:
            err_text = [f"{start} <= x <= {end}" for start, end in self.segment_intervals_raw]
            raise ValueError(
                f"Value {raw_val} is not in any of the specified intervals: {' OR '.join(err_text)}"
            )

    def get_offset_and_scaling_for_scaled(self, scaled_val: float) -> Tuple[float, float]:
        """Return the applicable offset and scaling factor for a given
        scaled value.
        """
        if isinstance(self._offset, (int, float)):
            assert isinstance(self._scale, (int, float))
            # global linear scaling
            return self._offset, self._scale

        assert isinstance(self._scale, list)
        for interval, offset, scale in zip(self.segment_intervals_scaled,
                                           self._offset,
                                           self._scale,):
            start, end = interval
            if start <= scaled_val <= end:
                return offset, scale
        else:
            err_text = [f"{start} <= x <= {end}" for start, end in self.segment_intervals_raw]
            raise ValueError(
                f"Value {scaled_val} is not in any of the specified intervals: {' OR '.join(err_text)}"
            )

    @property
    def offset(self) -> float:
        """Return the offset in the case of global linear scaling

        Raises a ```TypeError``` if piecewise linear scaling is used.

        (For piecewise linear scaling, use
        ```get_offset_and_scaling_for_{raw,scaled}()```)

        """
        if isinstance(self._offset, list):
            raise TypeError(".offset is not defined piecewise linear functions")
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
            raise TypeError(".scale is not defined piecewise linear functions")
        else:
            return self._scale

    @scale.setter
    def scale(self, new_scale: Union[float, List[float]]) -> None:
        """Set scale"""
        self._scale = new_scale

    def raw_to_scaled(self,
                      raw_value: Union[int, float],
                      decode_choices: bool = True) -> SignalValueType:
        """Convert an internal raw value according to the defined scaling or value table.

        :param raw:
            The raw value
        :param decode_choices:
            If `decode_choices` is ``False`` scaled values are not
            converted to choice strings (if available).
        :return:
            The calculated scaled value
        """

        # translate the raw value into a string if it is named and
        # translation requested
        if decode_choices and self.choices and raw_value in self.choices:
            assert isinstance(raw_value, int)
            return self.choices[raw_value]

        # scale the value
        offset, factor = self.get_offset_and_scaling_for_raw(raw_value)

        if factor == 1 and (isinstance(offset, int) or offset.is_integer()):
            # avoid unnecessary rounding error if the scaling factor is 1
            return raw_value + int(offset)

        return float(raw_value*factor + offset)

    def scaled_to_raw(self, scaled_value: SignalValueType) -> Union[int, float]:
        """Convert a scaled value to the internal raw value.

        :param scaled:
            The physical value.
        :return:
            The internal raw value.
        """

        # translate the scaled value into a number if it is an alias
        if isinstance(scaled_value, (str, NamedSignalValue)):
            return self.choice_string_to_number(str(scaled_value))

        # "unscale" the value. Note that this usually produces a float
        # value even if the raw value is supposed to be an
        # integer.
        offset, factor  = self.get_offset_and_scaling_for_scaled(scaled_value)

        if factor == 1 and (isinstance(offset, int) or offset.is_integer()):
            # avoid unnecessary rounding error if the scaling factor is 1
            result = scaled_value - int(offset)
        else:
            result = (scaled_value - offset)/factor

        if self.is_float:
            return float(result)
        else:
            return round(result)
