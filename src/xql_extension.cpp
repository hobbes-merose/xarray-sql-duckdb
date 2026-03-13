#define DUCKDB_EXTENSION_MAIN

#include "xql_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void XqlScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "XQL " + name.GetString() + " 🦆");
	});
}

inline void XqlOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "XQL " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto xql_scalar_function = ScalarFunction("xql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, XqlScalarFun);
	loader.RegisterFunction(xql_scalar_function);

	// Register another scalar function
	auto xql_openssl_version_scalar_function = ScalarFunction("xql_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, XqlOpenSSLVersionScalarFun);
	loader.RegisterFunction(xql_openssl_version_scalar_function);
}

void XqlExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string XqlExtension::Name() {
	return "xql";
}

std::string XqlExtension::Version() const {
#ifdef EXT_VERSION_XQL
	return EXT_VERSION_XQL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(xql, loader) {
	duckdb::LoadInternal(loader);
}
}
