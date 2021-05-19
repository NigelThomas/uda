/*
// $Id: //open/dt/steady/fennel/calctest/testUdf.cpp#1 $
// Aspen dataflow server
// Copyright (C) 2020-2020 SQLstream, Inc.
// Copyright (C) 2020 Guavus, Inc.
*/
#include "sqlstream/Udf.h"

using namespace sqlstream;
using fennel::SqlState;

// Some example UDFs

// simple UDF that takes two booleans and returns a boolean
class XorOp {
public:
    inline void operator()(CalculatorContext context, ResultRegister<bool> &resultReg, bool x, bool y) {
        resultReg = x ^ y;
    }
};
// INSTALL_UDF is used to export the UDF, so that it can be installed
// through sql.
// Parameters:
//     <name to use in sql CREATE FUNCTION>, <name of class>
// Valid result and parameter types are:
//     int8_t (TINYINT), int16_t (SMALLINT), int32_t (INTEGER), int64_t (BIGINT, DECIMAL or TIMESTAMP),
//     double (FLOAT or DOUBLE), bool (BOOLEAN),
//     char_t (CHAR), varchar_t (VARCHAR), binary_t (BINARY), varbinary_t (VARBINARY)
INSTALL_UDF(xorOp, XorOp)

// A templated UDF that supports multiple types
template<typename TYPE>
class AddOp {
    // uses gcc builtin to return true on overflow
    bool doAdd(TYPE x, TYPE y, TYPE &result) {
        if constexpr(std::is_integral<TYPE>::value) {
            return __builtin_add_overflow(x, y, &result);
        } else {
            // otherwise should be DOUBLE
            result = x + y;
            return std::isnan(result);
        }
    }
public:
    inline void operator()(CalculatorContext context, ResultRegister<TYPE> &resultReg, TYPE x, TYPE y) {
        TYPE result;
        if (doAdd(x, y, result)) {
            // log to trace stream
            context.logInfo("this is an info message zzzzzz");
            context.logWarning("this is a warning message zzzzzz");
            context.logSevere("this is a severe message zzzzzz");
            // throw an exception
            context.throwException(SqlState::instance().code22003());
        } else {
            resultReg = result;
        }
    }
};
// Install once for each type supported
INSTALL_UDF(addLongs, AddOp<int64_t>)
INSTALL_UDF(addInts, AddOp<int32_t>)
INSTALL_UDF(addShorts, AddOp<int16_t>)
INSTALL_UDF(addTinys, AddOp<int8_t>)
INSTALL_UDF(addDoubles, AddOp<double>)

class NegateLongOrNullOp {
public:
    inline void operator()(CalculatorContext context, ResultRegister<int64_t> &result, int64_t x) {
        if (x == 0) {
            result.toNull(); // this is how you explicitly return NULL - result will always be NULL if any arguments are NULL
        } else {
            result = -x;
        }
    }
};
INSTALL_UDF(negateLong, NegateLongOrNullOp)

class ConcatOp {
    // variables declared as fields persist between calls.
    std::string temp;
public:
    inline void operator()(CalculatorContext context, ResultRegister<varchar_t> &result, varchar_t x, varchar_t y) {
        temp.assign(x.data, x.size);
        temp.append(y.data, y.size);
        // For CHAR, VARCHAR, BINARY and VARBINARY, we support "reference",
        // which returns a value without copying it. Note, should only pass data
        // that pas been passed in or data declared as fields.
        // otherwise use the "=" operator.
        result.reference(temp.data(), temp.size());
    }
};
INSTALL_UDF(concat, ConcatOp)

// An example with several parameters.
// Note that the types in the function must match the types specified in INSTALL_UDF
class AddSome
{
public:
    inline void operator()(CalculatorContext context, ResultRegister<double> &resultReg, double w, int16_t x, int32_t y, int64_t z) {
       resultReg = w + x + y + z;
    }
};
INSTALL_UDF(addSome, AddSome)

// An example that convertsreinterprets varbinary to a different char or binary type
template<typename X>
class Varbinary2X
{
    using DataType = decltype(std::declval<X>().data);
public:
    inline void operator()(CalculatorContext context, ResultRegister<X> &resultReg, varbinary_t val) {
       // safe to use reference since val is passed in.
       // will throw exception if converting to char or varchar and data is not valid utf8
       resultReg.reference(reinterpret_cast<DataType>(val.data), val.size);
    }
};
INSTALL_UDF(varbinary2varchar, Varbinary2X<varchar_t>)
INSTALL_UDF(varbinary2char, Varbinary2X<char_t>)
INSTALL_UDF(varbinary2varbinary, Varbinary2X<varbinary_t>)
INSTALL_UDF(varbinary2binary, Varbinary2X<binary_t>)

// an example that gets the length in codepoints of a varchar.
class UnicodeLength
{
public:
    inline void operator()(CalculatorContext context, ResultRegister<int32_t> &resultReg, varchar_t val) {
       resultReg = val.unicodeLength();
    }
};
INSTALL_UDF(unicodeLength, UnicodeLength)

// Some example UDAs

// An example scaler UDA that returns NULL when all input rows are NULL.
// Thre return value will be the current value of the accumulator
class Adder : public Count0IsNullUda
{
  int32_t acc;
  bool doAdd(int32_t x, int32_t y, int32_t &result) {
    return __builtin_add_overflow(x, y, &result);
  }
public:
  // if initAdd is specified, then it will be called with the first non null value.
  void initAdd(CalculatorContext ctx, int32_t value) {
    acc = value;
  }
  void inline add(CalculatorContext ctx, int32_t value) {
    if (doAdd(acc, value, acc)) {
        ctx.throwException(SqlState::instance().code22003());
    }
  }
};
INSTALL_UDA(myAdd, Adder, int32_t)

// Similar version that does not use initAdd
class SimpleAdder : public Count0IsNullUda
{
  double acc;
public:

  inline SimpleAdder() : acc(0) {}

  inline void add(CalculatorContext ctx, double value) {
    //fprintf(stderr, "add %f, %f\n", acc, value);
    acc += value;
  }
};
INSTALL_UDA(simpleAdd, SimpleAdder, double)

// A non scaler UDA that's still "flat". The size never changes and the accumulator
// can be "trivially" copied.
// This computes mean and variance using the Welford algorithm.
class Welford : public Count0IsInitialUda
{
  double mean;
  int64_t count;
  double m2;
public:
  inline Welford() : mean(0), count(0), m2(0) {}
  
  inline void add(CalculatorContext ctx, double value) {
    count++;
    auto delta = value - mean;
    mean += delta / count;
    m2 += delta * (value - mean);
    if (std::isnan(m2) || std::isnan(mean)) {
        // could probably do this in getters for slightly better performance
        ctx.throwException(SqlState::instance().code22003());
    }
  }

  inline void getSampVariance(CalculatorContext ctx, const ResultRegister<double> &result) const {
    if (count < 2) {
      result.toNull();
    } else {
      result = m2 / (count - 1);
    }
  }
  inline void getPopVariance(CalculatorContext ctx, const ResultRegister<double> &result) const {
    if (count < 1) {
      result.toNull();
    } else {
      result = m2 / count;
    }
  }
  inline void getMean(CalculatorContext ctx, const ResultRegister<double> &result) const {
    if (count < 1) {
      result.toNull();
    } else {
      result = mean;
    }
  }
  inline void getCount(CalculatorContext ctx, const ResultRegister<int64_t> &result) const {
      result = count;
  }
};
// Install UDA
INSTALL_BASE_UDA(welford, Welford)
// Install getters. These will be the the actual functions that will be referenced in
// sql CREATE AGGREGATE FUNCTION
INSTALL_UDA_RESULT_FUNCTION(welfordSampVariance, welford, Welford, getSampVariance)
INSTALL_UDA_RESULT_FUNCTION(welfordPopVariance, welford, Welford, getPopVariance)
INSTALL_UDA_RESULT_FUNCTION(welfordMean, welford, Welford, getMean)
INSTALL_UDA_RESULT_FUNCTION(welfordCount, welford, Welford, getCount)

// A UDA that is not flat. We need to inherit from ComplexUda. UDA infrastructure will handle
// memory management.
class StringConcatter : public sqlstream::ComplexUda {
public:
  std::string acc;

  // this will be called when reusing aggregate
  inline void init() {
    acc.clear();
  }

  inline void add(CalculatorContext ctx, varchar_t value) {
    acc.append(value.data, value.size);
  }

  inline void getResult(CalculatorContext ctx, const ResultRegister<varchar_t> &result) const {
    // better would be to use "reference", but we'll use assignment just to show we can
    result = acc;
  }
};
INSTALL_BASE_UDA(concatter, StringConcatter)
INSTALL_UDA_RESULT_FUNCTION(strlist, concatter, StringConcatter, getResult)

// End sampleUdfs.cpp
