#define DUCKDB_EXTENSION_MAIN

#include "xql_extension.hpp"
#include "zarr_metadata.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

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

// ============================================================================
// read_zarr_metadata table function
// ============================================================================

struct ReadZarrMetadataState {
	std::vector<ZarrArrayMetadata> arrays;
	size_t current_index = 0;
};

static void ReadZarrMetadataFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.state_obj->Cast<ReadZarrMetadataState>();
	
	if (state.current_index >= state.arrays.size()) {
		output.SetCardinality(0);
		return;
	}
	
	// Determine how many rows to output
	idx_t count = std::min((idx_t)STANDARD_VECTOR_SIZE, (idx_t)(state.arrays.size() - state.current_index));
	
	// Allocate columns
	output.SetCardinality(count);
	auto &name_col = output.data[0];
	auto &shape_col = output.data[1];
	auto &dtype_col = output.data[2];
	auto &chunks_col = output.data[3];
	auto &compressor_col = output.data[4];
	auto &zarr_version_col = output.data[5];
	auto &fill_value_col = output.data[6];
	
	// Fill the columns
	for (idx_t i = 0; i < count; i++) {
		auto &array_meta = state.arrays[state.current_index + i];
		
		// Name
		StringVector::SetString(name_col, i, array_meta.name);
		
		// Shape - join as comma-separated string
		std::string shape_str;
		for (size_t j = 0; j < array_meta.shape.size(); j++) {
			if (j > 0) shape_str += ",";
			shape_str += std::to_string(array_meta.shape[j]);
		}
		StringVector::SetString(shape_col, i, shape_str);
		
		// Dtype
		StringVector::SetString(dtype_col, i, array_meta.dtype);
		
		// Chunks - join as comma-separated string
		std::string chunks_str;
		for (size_t j = 0; j < array_meta.chunks.size(); j++) {
			if (j > 0) chunks_str += ",";
			chunks_str += std::to_string(array_meta.chunks[j]);
		}
		StringVector::SetString(chunks_col, i, chunks_str);
		
		// Compressor
		StringVector::SetString(compressor_col, i, array_meta.compressor);
		
		// Zarr version
		zarr_version_col.SetValue(i, Value::INTEGER(array_meta.zarr_version));
		
		// Fill value
		StringVector::SetString(fill_value_col, i, array_meta.fill_value);
	}
	
	state.current_index += count;
}

static unique_ptr<FunctionData> ReadZarrMetadataBind(ClientContext &context, TableFunctionBindInfo &bind_info,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	// Define the output schema
	return_types.push_back(LogicalType::VARCHAR);   // name
	return_types.push_back(LogicalType::VARCHAR);   // shape
	return_types.push_back(LogicalType::VARCHAR);   // dtype
	return_types.push_back(LogicalType::VARCHAR);   // chunks
	return_types.push_back(LogicalType::VARCHAR);   // compressor
	return_types.push_back(LogicalType::INTEGER);  // zarr_version
	return_types.push_back(LogicalType::VARCHAR);  // fill_value
	
	names.push_back("name");
	names.push_back("shape");
	names.push_back("dtype");
	names.push_back("chunks");
	names.push_back("compressor");
	names.push_back("zarr_version");
	names.push_back("fill_value");
	
	return make_uniq<ReadZarrMetadataState>();
}

static unique_ptr<FunctionData> ReadZarrMetadataInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ReadZarrMetadataState>();
	
	// Get the path from the named parameters
	std::string path;
	
	// Look for path parameter
	for (const auto &named_param : input.named_parameters) {
		if (named_param.first == "path") {
			path = named_param.second.ToString();
			// Remove quotes if present
			if (path.size() >= 2 && path.front() == '\'' && path.back() == '\'') {
				path = path.substr(1, path.size() - 2);
			} else if (path.size() >= 2 && path.front() == '"' && path.back() == '"') {
				path = path.substr(1, path.size() - 2);
			}
			break;
		}
	}
	
	if (path.empty()) {
		throw Exception("read_zarr_metadata requires a 'path' parameter");
	}
	
	// Parse the zarr metadata
	ZarrMetadata metadata;
	metadata.Parse(path);
	
	if (!metadata.IsValid()) {
		throw Exception("Failed to read zarr metadata: " + metadata.GetError());
	}
	
	result->arrays = metadata.GetArrays();
	
	return std::move(result);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto xql_scalar_function = ScalarFunction("xql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, XqlScalarFun);
	loader.RegisterFunction(xql_scalar_function);

	// Register another scalar function
	auto xql_openssl_version_scalar_function = ScalarFunction("xql_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, XqlOpenSSLVersionScalarFun);
	loader.RegisterFunction(xql_openssl_version_scalar_function);
	
	// Register read_zarr_metadata table function
	TableFunction read_zarr_metadata_fn("read_zarr_metadata", {}, ReadZarrMetadataFunction, 
	                                    ReadZarrMetadataBind, ReadZarrMetadataInit);
	read_zarr_metadata_fn.named_parameters["path"] = LogicalType::VARCHAR;
	loader.RegisterFunction(read_zarr_metadata_fn);
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
