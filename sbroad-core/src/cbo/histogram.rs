//! Equi-height histogram.
//!
//! Module used to represent logic of applying and transforming histogram statistics during
//! CBO algorithms.

use crate::errors::{Entity, SbroadError};
use crate::ir::value::{value_to_decimal_or_error, Value};
use itertools::enumerate;
use std::str::FromStr;

/// Helper structure that represents pair of most common value in the column and its frequency.
#[derive(Debug, PartialEq, Clone)]
struct MostCommonValueWithFrequency {
    value: Value,
    frequency: f64,
}

impl MostCommonValueWithFrequency {
    #[allow(dead_code)]
    fn new(value: Value, frequency: f64) -> Self {
        MostCommonValueWithFrequency { value, frequency }
    }
}

/// Representation of histogram bucket.
#[derive(Clone, Debug, PartialEq)]
struct Bucket<'bucket> {
    /// From (left border) value of the bucket (not inclusive, except for the first bucket)
    pub from: &'bucket Value,
    /// To (right order) value of the bucket (inclusive)
    pub to: &'bucket Value,
    /// Bucket frequency.
    /// Represents the number of elements stored in the bucket.
    pub frequency: usize,
}

/// Representation of equi-height histogram.
///
/// It's assumed that if the histogram is present, then all
/// its fields are filled.
///
/// As soon as the biggest part of the logic is taken from
/// `PostgreSQL` implementation, you may see `PostgreSQL lines` comments
/// in some places. It means you can find
/// implementation of `PostgreSQL` logic by searching the provided text.
///
/// `PostgreSQL` version: `REL_15_2`
#[derive(Debug, PartialEq, Clone)]
pub struct Histogram<'histogram> {
    // Most common values and their frequencies.
    most_common: Vec<MostCommonValueWithFrequency>,
    /// Histogram buckets.
    ///
    /// **Note**: Values from mcv are not included in histogram buckets.
    ///
    /// Boundaries:
    /// * i = 0 -> [b_0; b_1] (where `from` field of the bucket is included)
    /// * i = 1 -> (b_1; b_2]
    /// * ...
    /// * i = n -> (b_(n-2); b_(n-1)]
    buckets: Vec<Bucket<'histogram>>,
    /// Fraction of NULL values among all column values.
    null_fraction: f64,
    /// Number of distinct values for the whole histogram.
    ///
    /// **Note**: It is easy during the histogram calculation
    /// phase to calculate ndv as soon as the elements have to be sorted
    /// in order to construct bucket_bounds Vec.
    ndv: usize,
    /// Number of elements added into histogram.
    ///
    /// **Note**: the number of values added into histogram don't
    /// have to be equal to the number of rows in the table as soon as
    /// some rows might have been added after the histogram was created.
    elements_count: usize,
}

/// Helper structure that represents `String` char sequence.
#[derive(Default, Debug, Clone)]
struct CharSequence<'chars> {
    inner: &'chars str,
}

impl<'chars> CharSequence<'chars> {
    /// Len of `inner` char sequence in bytes.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Min `u8` char stored in `inner`.
    ///
    /// Returns None in case `inner` is empty.
    fn min(&self) -> Option<u8> {
        self.inner.bytes().min().map_or_else(|| None, Some)
    }

    /// Max `u8` char stored in `inner`.
    ///
    /// Returns None in case `inner` is empty.
    fn max(&self) -> Option<u8> {
        self.inner.bytes().max().map_or_else(|| None, Some)
    }

    /// Helper function that converts `CharSequence` into `f64` value from 0 to 1
    /// considering given range bounds.
    /// It works with `CharSequence` subrange starting from the `from` index.
    ///
    /// It applies some kind of a reverse conversion algorithm.
    /// E.g. straight algorithm for usual ASCII string "cba" with the base of 255 (from 0 to 255)
    /// would traverse letters from right to left in a way like:
    /// 1.) "a" equals to 97. result += 97 * 255^1
    /// 2.) "b" equals to 98. result += 98 * 255^2
    /// 3.) "c" equals to 99. result += 99 * 255^3
    ///
    /// In this case we traverse the string in reverse order, every time decreasing the base and
    /// counting which part of the base range does our current char takes
    /// (subtracting the value from the `low_bound`):
    /// 1.) "c" equals to 99, the working range is [97, 122] so result += (99 - 97) / base^1
    /// 2.) "b" equals to 98, the working range is [97, 122] so result += (98 - 97) / base^2
    /// 3.) "a" equals to 97, the working range is [97, 122] so result += (97 - 97) / base^3
    ///
    /// Check the largest case (where all chars from the given char sequence equal to the
    /// `high_bound` value) in order to show that the resulting value is less than 1
    /// (here `base` considered to be just `high_bound - low_bound`):
    /// 1.) `(high_bound - low_bound) / (base + 1)     = base / (base + 1)`
    /// 2.) `(high_bound - low_bound) / (base + 1) ^ 2 = base / (base + 1)^2`
    /// ...
    /// `result = (base / (base + 1)) * (1 + 1/(base+1) + 1/(base+1)^2 + ...)`
    /// considering the fact that `sum of 1 / ((base + 1)^j), j = 0 to infinity = (base + 1) / base`
    /// we get that `result <= 1`
    ///
    /// `PostgreSQL` lines: `convert_one_string_to_scalar`, lines 4564-4603.
    #[allow(clippy::cast_precision_loss)]
    fn convert_chars_to_f64_from(&self, from: usize, low_bound: u8, high_bound: u8) -> f64 {
        let base = f64::from(high_bound - low_bound + 1);
        let mut result = 0.0;

        for (index, char) in enumerate(self.inner.bytes().skip(from).take(12)) {
            // We check that given `char` is in found bounds.
            // If somehow (in case we broaden our range from 0 to 127)
            // it's not, we made it equal to bound.
            let constrained_char = char.clamp(low_bound, high_bound);
            result +=
                f64::from(constrained_char - low_bound) * f64::powf(base, -((index + 1) as f64));
        }
        result
    }
}

impl<'chars> From<&'chars String> for CharSequence<'chars> {
    fn from(s: &'chars String) -> Self {
        Self { inner: s.as_str() }
    }
}

/// Helper structure that represents several `String`s packed in a vector.
#[derive(Debug)]
struct VecOfCharSequences<'chars> {
    vec: Vec<&'chars CharSequence<'chars>>,
}

impl<'chars> VecOfCharSequences<'chars> {
    fn new(bytes: Vec<&'chars CharSequence>) -> Self {
        VecOfCharSequences { vec: bytes }
    }

    /// Find the shortest bytes sequence len in `vec`.
    fn min_len(&self) -> usize {
        self.vec
            .iter()
            .map(|x| x.len())
            .min()
            .map_or_else(|| 0, |l| l)
    }

    /// Find the smallest `u8` value among all the `Bytes` stored in `vec`.
    fn low_bound(&self) -> Option<u8> {
        self.vec.iter().map(|x| x.min()).fold(None, |mut acc, x| {
            if let Some(min) = x {
                if let Some(value) = acc {
                    if min < value {
                        acc = Some(min);
                    }
                } else {
                    acc = Some(min);
                }
            }
            acc
        })
    }

    /// Find the greatest `u8` value among all the `Bytes` stored in `vec`.
    fn high_bound(&self) -> Option<u8> {
        self.vec.iter().map(|x| x.max()).fold(None, |mut acc, x| {
            if let Some(max) = x {
                if let Some(value) = acc {
                    if max > value {
                        acc = Some(max);
                    }
                } else {
                    acc = Some(max);
                }
            }
            acc
        })
    }

    fn iter(&self) -> Iter {
        Iter {
            inner: self,
            min_len: self.min_len(),
            index: 0,
        }
    }

    /// Find the greatest common prefix among all `Bytes` stored in `vec`.
    fn common_bytes_len(&self) -> usize {
        self.iter().count()
    }
}

/// Helper structure that represents iterator over `VecOfBytes`.
/// Used in order to calculate longest common prefix of `String`s packed into `inner`.
struct Iter<'bytes> {
    inner: &'bytes VecOfCharSequences<'bytes>,
    min_len: usize,
    index: usize,
}

impl<'bytes> Iterator for Iter<'bytes> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.min_len {
            return None;
        }

        let mut byte_value: Option<u8> = None;
        for bytes in &self.inner.vec {
            match bytes.inner.as_bytes().get(self.index) {
                Some(byte) => {
                    if byte_value.is_none() {
                        byte_value = Some(*byte);
                    } else if byte_value != Some(*byte) {
                        return None;
                    }
                }
                None => return None,
            }
        }

        self.index += 1;

        byte_value
    }
}

/// Helper function used to broaden working space bounds.
/// We presume that in case [`low_bound`; `high_bound`] range cover some part of [`low_char`; `high_char`]
/// range, then it should include it fully.
fn broaden_bounds(low_bound: &mut u8, high_bound: &mut u8, low_char: char, high_char: char) {
    let low_broadening = low_char as u8;
    let high_broadening = high_char as u8;

    if *low_bound <= high_broadening && *high_bound >= low_broadening {
        if *low_bound > low_broadening {
            *low_bound = low_broadening;
        }
        if *high_bound < high_broadening {
            *high_bound = high_broadening;
        }
    }
}

/// Sub-logic of `scale_value` related to Strings.
///
/// All the function main logic presumes that we are working with ASCII characters,
/// 1-byte characters that are represented with `u8` values from 0 to 255.
/// We transform given strings into such ASCII arrays, convert them into `f64` from 0 to 1 and
/// finally calculate wanted fraction.
///
/// `PostgreSQL` lines: `convert_string_to_scalar`, lines 4484-4561.
#[allow(dead_code)]
fn scale_strings(
    value: &String,
    left_bound: &String,
    right_bound: &String,
) -> Result<f64, SbroadError> {
    if value.is_empty() || left_bound.is_empty() || right_bound.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some(String::from("One of the passed strings is empty")),
        ));
    }

    let value_bytes = CharSequence::from(value);
    let left_bound_bytes = CharSequence::from(left_bound);
    let right_bound_bytes = CharSequence::from(right_bound);
    let vec_of_bytes =
        VecOfCharSequences::new(vec![&value_bytes, &left_bound_bytes, &right_bound_bytes]);

    // As soon as not all strings contain all ASCII characters from 0 to 255,
    // we want to reduce the range (base) that will be used for scaling.
    let low_bound = vec_of_bytes.low_bound();
    let high_bound = vec_of_bytes.high_bound();

    let (Some(mut low_bound), Some(mut high_bound)) = (low_bound, high_bound) else {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some(String::from("One of the passed strings is empty")),
        ));
    };

    broaden_bounds(&mut low_bound, &mut high_bound, '0', '9');
    broaden_bounds(&mut low_bound, &mut high_bound, 'A', 'Z');
    broaden_bounds(&mut low_bound, &mut high_bound, 'a', 'z');

    // In case our range is too small, we broaden it to the regular ASCII set.
    //
    // **Note**: In case our given values are smth like 230 (value), 235 and 240 (bounds)
    // the range will become [0; 127] and during `convert_chars_to_f64` all the values will just
    // be mapped to `high_bound` = 127. Nothing bad will happen.
    if high_bound - low_bound < 9 {
        low_bound = 0;
        high_bound = 127;
    }

    // We want to remove the longest common prefix from all of the input strings in order to
    // reduce the working chars space.
    let greatest_common_prefix_length = vec_of_bytes.common_bytes_len();

    // Convert strings with removed prefixes to `f64`.
    let value_f64 =
        value_bytes.convert_chars_to_f64_from(greatest_common_prefix_length, low_bound, high_bound);
    let low_bound_f64 = left_bound_bytes.convert_chars_to_f64_from(
        greatest_common_prefix_length,
        low_bound,
        high_bound,
    );
    let high_bound_f64 = right_bound_bytes.convert_chars_to_f64_from(
        greatest_common_prefix_length,
        low_bound,
        high_bound,
    );

    // Check that converted values are located adequately.
    if high_bound_f64 < low_bound_f64 {
        Err(
            SbroadError::Invalid(
                Entity::Value,
                Some(format!(
                    "Converted high_bound {high_bound_f64} must be greater than converted low_bound {low_bound_f64}")
                ),
            )
        )
    } else if value_f64 < low_bound_f64 || value_f64 > high_bound_f64 {
        Err(
            SbroadError::Invalid(
                Entity::Value,
                Some(format!(
                    "Converted value {value_f64} must lie between converted low_bound {low_bound_f64} and converted high_bound {high_bound_f64}")
                ),
            )
        )
    } else {
        Ok((value_f64 - low_bound_f64) / (high_bound_f64 - low_bound_f64))
    }
}

/// Helper function that calculates the fraction that value takes in a histogram bucket.
/// [++++++++++++++++&-------------------------]
/// `left_bound`   `value`                  `right_bound`
///
/// We assume here that only two value type categories are available for scaling:
/// 1.) Strings   (that are scaled using `scale_strings` function)
/// 2.) Numerical (that are scaled being casted to decimals)
/// I.e. in case Boolean, Null or Tuple were passed, function will return an error.
///
/// `PostgreSQL` lines: `convert_to_scalar`, lines 4275-4461.
#[allow(dead_code)]
pub(crate) fn scale_values(
    value: &Value,
    left_bound: &Value,
    right_bound: &Value,
) -> Result<f64, SbroadError> {
    if let (Value::String(v), Value::String(l), Value::String(r)) = (value, left_bound, right_bound)
    {
        scale_strings(v, l, r)
    } else {
        let value_decimal = value_to_decimal_or_error(value);
        let left_decimal = value_to_decimal_or_error(left_bound);
        let right_decimal = value_to_decimal_or_error(right_bound);
        if let (Ok(value_decimal), Ok(left_decimal), Ok(right_decimal)) =
            (value_decimal, left_decimal, right_decimal)
        {
            let decimal_res = (value_decimal - left_decimal) / (right_decimal - left_decimal);
            let f64_res = f64::from_str(&decimal_res.to_string());
            if let Ok(f64_res) = f64_res {
                Ok(f64_res)
            } else {
                Err(SbroadError::Invalid(
                    Entity::Value,
                    Some(format!(
                        "Error occurred when trying to case decimal {decimal_res} to f64"
                    )),
                ))
            }
        } else {
            Err(
                SbroadError::Invalid(
                    Entity::Value,
                    Some(format!(
                        "Scaling may be done only String to String or Numerical to Numerical. Values {value:?}, {left_bound:?} and {right_bound:?} were passed"
                    )),
                )
            )
        }
    }
}

#[cfg(test)]
mod tests;
