#include <box/execute.h>
#include "sql.h"
#include "box/sql.h"
#include "lua/msgpack.h"

#include "box/sql/sqliteInt.h"
#include "box/info.h"
#include "lua/utils.h"
#include "info.h"
#include "mpstream.h"
#include "net_box.h"

static int
lua_sql_debug(struct lua_State *L)
{
	struct info_handler info;
	luaT_info_handler_create(&info, L);
	sql_debug_info(&info);
	return 1;
}

int
lbox_sql_execute(lua_State *L)
{
	sqlite3 *db = sql_get();
	if (db == NULL)
		return luaL_error(L, "not ready");

	size_t length;
	const char *sql = lua_tolstring(L, 1, &length);
	if (sql == NULL)
		return luaL_error(L, "usage: box:call(sqlstring)");

	struct region *region = &fiber()->gc;
	char *raw = region_alloc(region, mp_sizeof_str(length));
	if (raw == NULL) {
		diag_set(OutOfMemory, mp_sizeof_str(length), "region_alloc",
			 "str_req");
		return luaT_error(L);
	}
	mp_encode_str(raw, sql, length);

	struct sql_response response;
	memset(&response, 0, sizeof(struct sql_response));
	struct sql_request request;
	memset(&request, 0, sizeof(struct sql_request));
	request.sql_text = raw;
	if (sql_prepare_and_execute(&request, &response, region) != 0) {
		lua_pushstring(L, sqlite3_errmsg(db));
		return lua_error(L);
	}

	uint32_t size;
	raw = sql_response_encode(region, &response, &size);
	port_destroy(&response.port);
	struct sqlite3_stmt *stmt = response.prep_stmt;
	assert(stmt != NULL);
	sqlite3_finalize(stmt);
	if (raw == NULL)
		return luaT_error(L);

	*(const char **)luaL_pushcdata(L, LUA_TCDATA) = raw;
	lua_replace(L, 1);
	lua_settop(L, 1);
	netbox_decode_execute(L);
	lua_pop(L, 1);
	return 1;
}

void
box_lua_sqlite_init(struct lua_State *L)
{
	static const struct luaL_Reg module_funcs [] = {
		{"debug", lua_sql_debug},
		{NULL, NULL}
	};

	/* used by lua_sql_execute via upvalue */
	lua_createtable(L, 0, 1);
	lua_pushstring(L, "sequence");
	lua_setfield(L, -2, "__serialize");

	luaL_openlib(L, "box.sql", module_funcs, 1);
	lua_pop(L, 1);
}

