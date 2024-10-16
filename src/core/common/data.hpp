/*
 *  Copyright (c) 2021, The OpenThread Authors.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the copyright holder nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file
 *   This file includes definitions for a `Data` and `MutableData`.
 */

#ifndef DATA_HPP_
#define DATA_HPP_

#include "openthread-core-config.h"

#include <stdint.h>
#include <string.h>

#include "common/clearable.hpp"
#include "common/code_utils.hpp"
#include "common/const_cast.hpp"
#include "common/equatable.hpp"
#include "common/error.hpp"
#include "common/num_utils.hpp"
#include "common/type_traits.hpp"

namespace ot {

/**
 * Type is used as the template parameter in `Data` and `MutableData` to indicate the `uint` type to
 * use for the data length.
 *
 */
enum DataLengthType : uint8_t
{
    kWithUint8Length,  ///< Use `uint8_t` for data length.
    kWithUint16Length, ///< Use `uint16_t` for data length
};

/**
 * Specifies a function pointer which matches two given bytes.
 *
 * Such a function is used as a parameter in `Data::MatchesByteIn()` method. This can be used to relax the definition
 * of a match when comparing data bytes, e.g., can be used for case-insensitive string comparison.
 *
 * @param[in] aFirst   A first byte.
 * @param[in] aSecond  A second byte.
 *
 * @retval TRUE  if @p aFirst matches @p aSecond.
 * @retval FALSE if @p aFirst does not match @p aSecond.
 *
 */
typedef bool (*ByteMatcher)(uint8_t aFirst, uint8_t aSecond);
typedef int (*LexicographicallyByteMatcher)(uint8_t aFirst, uint8_t aSecond);

/**
 * Implements common utility methods used by `Data` and `MutableData`.
 *
 */
class DataUtils
{
protected:
    DataUtils(void) = default;
    static bool MatchBytes(const uint8_t *aFirstBuffer,
                           const uint8_t *aSecondBuffer,
                           uint16_t       aLength,
                           ByteMatcher    aMatcher);

    static int CompareBytesLexicographically(const uint8_t               *aFirstBuffer,
                                             const uint8_t               *aSecondBuffer,
                                             uint16_t                     aLength,
                                             LexicographicallyByteMatcher aMatcher);
};

template <DataLengthType kDataLengthType> class MutableData;

/**
 * Represents a generic `Data` which is simply a wrapper over a pointer to a buffer with a given data length.
 *
 * The data length can be either `uint8_t` or `uint16_t` (determined by the template parameter `kDataLengthType`).
 *
 * While a `Data` instance itself can change (for example, it can be updated to point to another buffer), it always
 * treats the content of the buffer as immutable.
 *
 * A `Data` instance MUST be initialized (using any of the `Init()` methods) before calling any other methods on the
 * instance (e.g., `GetBytes()` or `GetLength()`), otherwise the behavior is undefined.
 *
 * @tparam kDataLengthType   Determines the data length type (`uint8_t` or `uint16_t`).
 *
 */
template <DataLengthType kDataLengthType>
class Data : public Clearable<Data<kDataLengthType>>, public Unequatable<Data<kDataLengthType>>, private DataUtils
{
    friend class MutableData<kDataLengthType>;

public:
    /**
     * Represents the data length type (`uint8_t` or `uint16_t`).
     *
     */
    using LengthType = typename TypeTraits::Conditional<kDataLengthType == kWithUint8Length, uint8_t, uint16_t>::Type;

    /**
     * Initializes the `Data` to point to a given buffer with a given length.
     *
     * @param[in] aBuffer   A pointer to a buffer containing the data.
     * @param[in] aLength   The data length (number of bytes in @p aBuffer)
     *
     */
    void Init(const void *aBuffer, LengthType aLength)
    {
        mBuffer = static_cast<const uint8_t *>(aBuffer);
        mLength = aLength;
    }

    /**
     * Initializes the `Data` to point to a range of bytes in a given buffer.
     *
     * The range is specified by the pointers to its start @p aStart and its end @p aEnd. `Data` will point to the
     * bytes in the buffer from @p aStart up to but excluding @p aEnd (i.e., `aStart <= bytes < aEnd`).
     *
     * @param[in] aStart  Pointer to the start of the range.
     * @param[in] aEnd    Pointer to the end of the range.
     *
     */
    void InitFromRange(const uint8_t *aStart, const uint8_t *aEnd)
    {
        Init(aStart, static_cast<LengthType>(aEnd - aStart));
    }

    /**
     * Initializes the `Data` to point to the content of an object.
     *
     * @tparm ObjectType   The object type (MUST not be a pointer type).
     *
     * @param[in] aObject   The object to initialize the `Data` with.
     *
     */
    template <typename ObjectType> void InitFrom(const ObjectType &aObject)
    {
        static_assert(!TypeTraits::IsPointer<ObjectType>::kValue, "ObjectType MUST not be a pointer");
        Init(&aObject, sizeof(aObject));
    }

    /**
     * Returns a pointer to the data bytes buffer.
     *
     * @returns A pointer to the data bytes buffer (can be `nullptr` if `Data` is cleared).
     *
     */
    const uint8_t *GetBytes(void) const { return mBuffer; }

    /**
     * Returns the data length.
     *
     * @returns The data length (number of bytes).
     *
     */
    LengthType GetLength(void) const { return mLength; }

    /**
     * Sets the data length.
     *
     * @param[in] aLength   The data length (number of bytes).
     *
     */
    void SetLength(LengthType aLength) { mLength = aLength; }

    /**
     * Copies the `Data` bytes to a given buffer.
     *
     * It is up to the caller to ensure that @p aBuffer has enough space for the current data length.
     *
     * @param[out] aBuffer  The buffer to copy the bytes into.
     *
     */
    void CopyBytesTo(void *aBuffer) const { memcpy(aBuffer, mBuffer, mLength); }

    /**
     * Compares the `Data` content with the bytes from a given buffer.
     *
     * It is up to the caller to ensure that @p aBuffer has enough bytes to compare with the current data length.
     *
     * @param[in] aBuffer   A pointer to a buffer to compare with the data.
     *
     * @retval TRUE   The `Data` content matches the bytes in @p aBuffer.
     * @retval FALSE  The `Data` content does not match the byes in @p aBuffer.
     *
     */
    bool MatchesBytesIn(const void *aBuffer) const { return memcmp(mBuffer, aBuffer, mLength) == 0; }

    /**
     * Compares the `Data` content with the bytes from a given buffer using a given `Matcher` function.
     *
     * It is up to the caller to ensure that @p aBuffer has enough bytes to compare with the current data length.
     *
     * @param[in] aBuffer   A pointer to a buffer to compare with the data.
     * @param[in] aMatcher  A `ByteMatcher` function to match the bytes. If `nullptr`, bytes are compared directly.
     *
     * @retval TRUE   The `Data` content matches the bytes in @p aBuffer.
     * @retval FALSE  The `Data` content does not match the byes in @p aBuffer.
     *
     */
    bool MatchesBytesIn(const void *aBuffer, ByteMatcher aMatcher)
    {
        return MatchBytes(mBuffer, static_cast<const uint8_t *>(aBuffer), mLength, aMatcher);
    }

    /**
     * This method compares the bytes from a given buffer using a given `Matcher` function with the `Data` content.
     *
     * It is up to the caller to ensure that @p aBuffer has enough bytes to compare with the current data length.
     *
     * @param[in] aBuffer   A pointer to a buffer to compare with the data.
     * @param[in] aMatcher  A `ByteMatcher` function to match the bytes. If `nullptr`, bytes are compared directly.
     *
     * @retval < 0    First different byte in @p aBuffer is less than byte in 'Data'.
     * @retval = 0    The bytes in @p aBuffer match the 'Data' content.
     * @retval > 0    First different byte in @p aBuffer is greater than byte in 'Data'.
     *
     */
    int CompareBytesLexicographicallyIn(const void *aBuffer, LexicographicallyByteMatcher aMatcher)
    {
        return CompareBytesLexicographically(static_cast<const uint8_t *>(aBuffer), mBuffer, mLength, aMatcher);
    }

    /**
     * This method overloads operator `==` to compare the `Data` content with the content from another one.
     *
     * @param[in] aOtherData   The other `Data` to compare with.
     *
     * @retval TRUE   The two `Data` instances have matching content (same length and same bytes).
     * @retval FALSE  The two `Data` instances do not have matching content.
     *
     */
    bool operator==(const Data &aOtherData) const
    {
        return (mLength == aOtherData.mLength) && MatchesBytesIn(aOtherData.mBuffer);
    }

    /**
     * Checks whether the `Data` starts with the same byte content as from another `Data` instance.
     *
     * Checks that the `Data` instance contains the same bytes as @p aOtherData but allows it to have
     * additional bytes at the end.
     *
     * @param[in] aOtherData  The other `Data` to compare with.
     *
     * @retval TRUE   This `Data` starts with the same byte content as in @p aOtherData.
     * @retval FALSE  This `Data` does not start with the same byte content as in @p aOtherData.
     *
     */
    bool StartsWith(const Data &aOtherData) const
    {
        return (mLength >= aOtherData.mLength) && aOtherData.MatchesBytesIn(mBuffer);
    }

private:
    const uint8_t *mBuffer;
    LengthType     mLength;
};

/**
 * Represents a generic `MutableData` which is simply a wrapper over a pointer to a buffer with a given data
 * length.
 *
 * It inherits from `Data` but unlike `Data` which treats its buffer content as immutable, `MutableData` allows its
 * data buffer content to be changed.
 *
 * A `MutableData` instance MUST be initialized (using any of the `Init()` methods) before calling any other methods
 * (e.g., `GetBytes()` or `GetLength()`), otherwise the behavior is undefined.

 *
 */
template <DataLengthType kDataLengthType> class MutableData : public Data<kDataLengthType>
{
    using Base = Data<kDataLengthType>;
    using Base::mBuffer;
    using Base::mLength;

public:
    /**
     * Represents the data length type (`uint8_t` or `uint16_t`).
     *
     */
    using LengthType = typename Base::LengthType;

    /**
     * Initializes the `MutableData` to point to a given buffer with a given length.
     *
     * @param[in] aBuffer   A pointer to a buffer containing the data.
     * @param[in] aLength   The data length (number of bytes in @p aBuffer)
     *
     */
    void Init(void *aBuffer, LengthType aLength) { Base::Init(aBuffer, aLength); }

    /**
     * Initializes the `MutableData` to point to a range of bytes in a given buffer.
     *
     * The range is specified by the pointers to its start @p aStart and its end @p aEnd. `Data` will point to the
     * bytes in the buffer from @p aStart up to but excluding @p aEnd (i.e., `aStart <= bytes < aEnd`).
     *
     * @param[in] aStart  Pointer to the start of the range.
     * @param[in] aEnd    Pointer to the end of the range.
     *
     */
    void InitFormRange(uint8_t *aStart, uint8_t *aEnd) { Base::InitFormRange(aStart, aEnd); }

    /**
     * Initializes the `MutableData` to point to the content of an object.
     *
     * @tparm ObjectType   The object type (MUST not be a pointer type).
     *
     * @param[in] aObject   The object to initialize the `MutableData` with.
     *
     */
    template <typename ObjectType> void InitFrom(ObjectType &aObject)
    {
        static_assert(!TypeTraits::IsPointer<ObjectType>::kValue, "ObjectType MUST not be a pointer");
        Init(&aObject, sizeof(aObject));
    }

    /**
     * Returns a pointer to the data bytes buffer.
     *
     * @returns A pointer to the data bytes buffer (can be `nullptr` if `Data` is empty or uninitialized).
     *
     */
    uint8_t *GetBytes(void) { return AsNonConst(Base::GetBytes()); }

    /**
     * Returns a pointer to the data bytes buffer.
     *
     * @returns A pointer to the data bytes buffer (can be `nullptr` if `Data` is empty or uninitialized).
     *
     */
    const uint8_t *GetBytes(void) const { return Base::GetBytes(); }

    /**
     * Clears all the bytes (sets them to zero) in the buffer pointed by the `MutableData`.
     *
     */
    void ClearBytes(void) { memset(GetBytes(), 0, mLength); }

    /**
     * Copies the bytes from a given buffer into the `MutableData` buffer.
     *
     * If the current `MutableData` length is larger than or equal to @p aLength, then all the bytes are copied
     * from @p aBuffer into the buffer of `MutableData` and the `MutableData`'s length is changed to @p aLength.
     *
     * If the current `MutableData` length is smaller than @p aLength, then the method returns `kErrorNoBufs` but still
     * copies as many bytes as can fit.
     *
     * @param[in] aBuffer  A pointer to a buffer to copy from.
     * @param[in] aLength  The length of @p aBuffer (number of bytes).
     *
     * @retval kErrorNone    Successfully copied the bytes into `MutableData` buffer and adjusted its length.
     * @retval kErrorNoBufs  `MutableData` buffer cannot fit the given @p aLength bytes.
     *
     */
    Error CopyBytesFrom(const uint8_t *aBuffer, LengthType aLength)
    {
        Error error = (mLength >= aLength) ? kErrorNone : kErrorNoBufs;

        mLength = Min(mLength, aLength);
        memcpy(AsNonConst(mBuffer), aBuffer, mLength);

        return error;
    }

    /**
     * Copies the bytes from an given `Data` instance into the `MutableData` buffer.
     *
     * If the current `MutableData` length is larger than or equal to the @p aData length, then all the bytes are copied
     * from @p aData into the buffer of `MutableData` and the `MutableData`'s length is adjusted accordingly.
     *
     * If the current `MutableData` length is smaller than @p aData length, then as many bytes as can fit are copied
     * and the method returns `kErrorNoBufs`.
     *
     * @param[in] aData      A `Data` instance to copy the content from.
     *
     * @retval kErrorNone    Successfully copied the bytes into `MutableData` buffer and adjusted its length.
     * @retval kErrorNoBufs  `MutableData` buffer cannot fit the given @p aData bytes.
     *
     */
    Error CopyBytesFrom(const Data<kDataLengthType> &aData)
    {
        return CopyBytesFrom(aData.GetBytes(), aData.GetLength());
    }
};

} // namespace ot

#endif // DATA_HPP_
