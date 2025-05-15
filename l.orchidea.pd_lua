-- l.orchidea - fast, reliable CSV-backed sample lookup for Pd (pdlua)
-- Agora com cache global do CSV: lê apenas uma vez por sessão Pd, a menos que você chame [reload]

local orchidea = pd.Class:new():register("l.orchidea")

-- Column names we expect in the CSV
local COL_INST = "Instrument (in full)"
local COL_TECH = "Technique (in full)"
local COL_PITCH = "Pitch"
local COL_DYN = "Dynamics"
local COL_PATH = "Path"

-- Cache global entre instâncias e recargas do script (persistente na sessão Pd)
local ORCHIDEA_CACHE = rawget(_G, "__ORCHIDEA_CACHE__") or {}
_G.__ORCHIDEA_CACHE__ = ORCHIDEA_CACHE

-- ─────────────────────────────────────
-- helpers

local function trim(s)
	if type(s) ~= "string" then
		return s
	end
	return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function joinpath(a, b)
	if not a or a == "" then
		return b
	end
	if not b or b == "" then
		return a
	end
	local ends_slash = a:sub(-1) == "/" or a:sub(-1) == "\\"
	local starts_slash = b:sub(1, 1) == "/" or b:sub(1, 1) == "\\"
	if ends_slash and starts_slash then
		return a .. b:sub(2)
	elseif ends_slash or starts_slash then
		return a .. b
	else
		return a .. "/" .. b
	end
end

local function keys_sorted(t)
	local arr = {}
	for k in pairs(t) do
		arr[#arr + 1] = k
	end
	table.sort(arr)
	return arr
end

-- Robust CSV line parser (handles quoted fields, escaped quotes, commas in quotes)
local function parse_csv_line(line)
	local res = {}
	local field = {}
	local i, n = 1, #line
	local in_quotes = false

	while i <= n do
		local c = line:sub(i, i)
		if in_quotes then
			if c == '"' then
				local nextc = line:sub(i + 1, i + 1)
				if nextc == '"' then
					field[#field + 1] = '"'
					i = i + 1
				else
					in_quotes = false
				end
			else
				field[#field + 1] = c
			end
		else
			if c == '"' then
				in_quotes = true
			elseif c == "," then
				res[#res + 1] = table.concat(field)
				field = {}
			else
				field[#field + 1] = c
			end
		end
		i = i + 1
	end
	res[#res + 1] = table.concat(field)
	return res
end

-- ─────────────────────────────────────
-- CSV loading and indexing (uncached)

local function add_to_index(index, entry)
	local inst = entry[COL_INST] or ""
	local tech = entry[COL_TECH] or ""
	local pitch = entry[COL_PITCH] or ""
	local dyn = entry[COL_DYN] or ""
	local path = entry[COL_PATH] or ""

	if inst == "" or tech == "" or pitch == "" or path == "" then
		return
	end

	index[inst] = index[inst] or {}
	index[inst][tech] = index[inst][tech] or {}
	index[inst][tech][pitch] = index[inst][tech][pitch] or {}
	index[inst][tech][pitch][dyn] = index[inst][tech][pitch][dyn] or {}
	table.insert(index[inst][tech][pitch][dyn], path)
end

local function load_csv_uncached(csvpath)
	local file = io.open(csvpath, "r")
	if not file then
		return false, ("CSV file not found: %s"):format(tostring(csvpath))
	end

	local headers = nil
	local header_idx = {}
	local data = {}
	local index = {}

	for raw in file:lines() do
		local line = raw:gsub("\r$", "")
		if line ~= "" then
			local row = parse_csv_line(line)
			if not headers then
				headers = row
				for i, h in ipairs(headers) do
					header_idx[trim(h)] = i
				end
				local required = { COL_INST, COL_TECH, COL_PITCH, COL_DYN, COL_PATH }
				for _, col in ipairs(required) do
					if not header_idx[col] then
						file:close()
						return false, ("Missing required CSV column '%s'"):format(col)
					end
				end
			else
				local entry = {
					[COL_INST] = trim(row[header_idx[COL_INST]] or ""),
					[COL_TECH] = trim(row[header_idx[COL_TECH]] or ""),
					[COL_PITCH] = trim(row[header_idx[COL_PITCH]] or ""),
					[COL_DYN] = trim(row[header_idx[COL_DYN]] or ""),
					[COL_PATH] = trim(row[header_idx[COL_PATH]] or ""),
				}
				data[#data + 1] = entry
				add_to_index(index, entry)
			end
		end
	end

	file:close()
	return true, { data = data, index = index, headers = headers }
end

-- Cached loader: lê apenas uma vez por caminho
local function get_cached_or_load(csvpath)
	local cached = ORCHIDEA_CACHE[csvpath]
	if cached and cached.data and next(cached.data) ~= nil then
		return true, cached
	end
	local ok, payload_or_err = load_csv_uncached(csvpath)
	if not ok then
		return false, payload_or_err
	end
	ORCHIDEA_CACHE[csvpath] = payload_or_err
	return true, payload_or_err
end

-- ─────────────────────────────────────
function orchidea:initialize(_, args)
	self.inlets = 1
	self.outlets = 1

	-- Optional creation args: [l.orchidea <instrument> <technique>]
	if args and #args >= 1 then
		self.instrument = args[1]
	end
	if args and #args >= 2 then
		self.technique = args[2]
	end

	-- Resolve paths for sidecar files (CSV + CFG)
	local basepath
	if pd._pathnames and pd._pathnames["l.orchidea"] then
		basepath = pd._pathnames["l.orchidea"]
	end

	if not basepath then
		self:error("[l.orchidea] Unable to resolve object base path; pd._pathnames not available.")
	end

	-- Load Orchidea root path from cfg (if present)
	local cfg_path = basepath and (basepath .. ".cfg") or nil
	if cfg_path then
		local f = io.open(cfg_path, "r")
		if f then
			local content = f:read("*a") or ""
			f:close()
			self.orchidea_path = trim(content)
		end
	end
	if not self.orchidea_path or self.orchidea_path == "" then
		self:error("[l.orchidea] Set orchidea path with 'setpath' to use the object.")
	end

	-- CSV path
	local csv_path = basepath and (basepath .. ".csv") or nil
	if not csv_path then
		self:error("[l.orchidea] CSV path could not be determined.")
		return true
	end

	-- Load from cache or parse once
	local ok, payload_or_err = get_cached_or_load(csv_path)
	if not ok then
		self:error("[l.orchidea] " .. tostring(payload_or_err))
	else
		self.data = payload_or_err.data
		self.index = payload_or_err.index
	end

	return true
end

-- Lookup with optional dynamics (nil dyn = match any)
function orchidea:lookup(instrument, technique, pitch, dyn)
	local t1 = self.index and self.index[instrument]
	if not t1 then
		return {}
	end
	local t2 = t1[technique]
	if not t2 then
		return {}
	end
	local t3 = t2[pitch]
	if not t3 then
		return {}
	end

	local results = {}
	if dyn and dyn ~= "" then
		local lst = t3[dyn]
		if lst then
			for i = 1, #lst do
				results[#results + 1] = lst[i]
			end
		end
	else
		for _, lst in pairs(t3) do
			for i = 1, #lst do
				results[#results + 1] = lst[i]
			end
		end
	end
	return results
end

-- ─────────────────────────────────────
function orchidea:in_1_inst(args)
	self.instrument = args and args[1] or nil
end

function orchidea:in_1_tech(args)
	self.technique = args and args[1] or nil
end

function orchidea:in_1_note(args)
	if not args or #args < 1 then
		self:error("[l.orchidea] note: missing arguments (pitch [dyn])")
		return
	end
	if not self.instrument or not self.technique then
		self:error("[l.orchidea] please set 'inst' and 'tech' before sending 'note'")
		return
	end

	local note = args[1]
	local dyn = (#args >= 2) and args[2] or nil

	local relpaths = self:lookup(self.instrument, self.technique, note, dyn)
	if #relpaths == 0 then
		self:error(
			string.format(
				"[l.orchidea] no matches for Instrument='%s' Technique='%s' Pitch='%s'%s",
				tostring(self.instrument),
				tostring(self.technique),
				tostring(note),
				dyn and (" Dyn='" .. tostring(dyn) .. "'") or ""
			)
		)
		return
	end

	local out = {}
	if self.orchidea_path and self.orchidea_path ~= "" then
		for i = 1, #relpaths do
			out[i] = joinpath(self.orchidea_path, relpaths[i])
		end
	else
		for i = 1, #relpaths do
			out[i] = relpaths[i]
		end
	end

	self:outlet(1, "list", out)
end

-- Lista técnicas, dinâmicas e notas disponíveis para um instrumento (logs via pd.post)
function orchidea:in_1_list_techdyn(args)
	local inst = (args and args[1]) or self.instrument
	if not inst or inst == "" then
		self:error("[l.orchidea] list_techdyn: missing instrument (set with 'inst' or pass as argument)")
		return
	end
	local inst_idx = self.index and self.index[inst]
	if not inst_idx then
		self:error("[l.orchidea] list_techdyn: unknown instrument: " .. tostring(inst))
		return
	end

	pd.post("")
	local techs_sorted = keys_sorted(inst_idx)
	pd.post("Techniques")
	pd.post("    " .. table.concat(techs_sorted, " "))

	local overall_dyn_set = {}
	local pertech_dyn = {}

	local overall_note_set = {}
	local pertech_notes = {}

	for _, tech in ipairs(techs_sorted) do
		local tech_tbl = inst_idx[tech]

		local dyn_set = {}
		local note_set = {}

		for pitch, pitch_tbl in pairs(tech_tbl) do
			if pitch and pitch ~= "" then
				note_set[pitch] = true
				overall_note_set[pitch] = true
			end
			for dyn, _ in pairs(pitch_tbl) do
				if dyn and dyn ~= "" then
					dyn_set[dyn] = true
					overall_dyn_set[dyn] = true
				end
			end
		end

		local dyn_arr = {}
		for d in pairs(dyn_set) do
			dyn_arr[#dyn_arr + 1] = d
		end
		table.sort(dyn_arr)
		pertech_dyn[tech] = dyn_arr

		local note_arr = {}
		for n in pairs(note_set) do
			note_arr[#note_arr + 1] = n
		end
		table.sort(note_arr)
		pertech_notes[tech] = note_arr
	end

	local overall_dyn = {}
	for d in pairs(overall_dyn_set) do
		overall_dyn[#overall_dyn + 1] = d
	end
	table.sort(overall_dyn)
	pd.post("")
	pd.post("Dynamics")
	pd.post("    " .. table.concat(overall_dyn, " "))

	local overall_notes = {}
	for n in pairs(overall_note_set) do
		overall_notes[#overall_notes + 1] = n
	end
	table.sort(overall_notes)
	pd.post("")
	pd.post("Notes")
	pd.post("    " .. table.concat(overall_notes, " "))

	pd.post("")
	for _, tech in ipairs(techs_sorted) do
		local dyn_atoms = { tech, "=>", "[" }
		local darr = pertech_dyn[tech] or {}
		for i = 1, #darr do
			dyn_atoms[#dyn_atoms + 1] = darr[i]
		end
		dyn_atoms[#dyn_atoms + 1] = "]"
		pd.post("    " .. table.concat(dyn_atoms, " "))

		local note_atoms = { tech, "(notes)", "=>", "[" }
		local narr = pertech_notes[tech] or {}
		for i = 1, #narr do
			note_atoms[#note_atoms + 1] = narr[i]
		end
		note_atoms[#note_atoms + 1] = "]"
		pd.post("    " .. table.concat(note_atoms, " "))
	end
end

function orchidea:in_1_setpath(args)
	if not args or #args == 0 then
		self:error("[l.orchidea] setpath: missing path")
		return
	end
	local path = trim(table.concat(args, " "))
	self.orchidea_path = path

	local basepath = pd._pathnames and pd._pathnames["l.orchidea"] or nil
	if not basepath then
		self:error("[l.orchidea] could not persist path: base path not available")
		return
	end

	local fullpath = basepath .. ".cfg"
	local file = io.open(fullpath, "w")
	if file then
		file:write(path)
		file:close()
	else
		self:error("[l.orchidea] failed to open cfg for writing: " .. fullpath)
	end
end

-- Reload CSV/index limpando o cache para este arquivo
function orchidea:in_1_reload()
	local basepath = pd._pathnames and pd._pathnames["l.orchidea"] or nil
	if not basepath then
		self:error("[l.orchidea] reload: base path not available")
		return
	end
	local csv_path = basepath .. ".csv"
	ORCHIDEA_CACHE[csv_path] = nil
	local ok, payload_or_err = get_cached_or_load(csv_path)
	if not ok then
		self:error("[l.orchidea] reload failed: " .. tostring(payload_or_err))
		return
	end
	self.data = payload_or_err.data
	self.index = payload_or_err.index
	pd.post("[l.orchidea] reloaded and cache refreshed.")
end
