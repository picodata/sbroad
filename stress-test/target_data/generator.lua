#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local params = { ... }

if #params ~= 1 then
    print("Не задано кол-во записей для генерации")
    os.exit(1)
end

local start_time = clock.time();

local record_count = params[1]

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")

local config = {
    spaces = {
        vehicle_actual = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin2",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclechassisnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclereleaseyear",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoctypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdocissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoccomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptstypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptscomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodycolor",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrand",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrandmodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodynum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecost",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegasequip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleproducername",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegrossmass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclesteeringwheeltypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclekpptype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletransmissiontype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecategory",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypeunit",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleecoclass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclespecfuncname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenclosedvolume",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginenum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepower",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepowerkw",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginetype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdrestrictiondate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvalnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaldate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaltype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "utilizationfeename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocissue",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocrestriction",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerorgname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerinn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerogrn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerkpp",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressindex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdepartmentname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationattorney",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationlising",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdertypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsnils",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressdistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressbuilding",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructureid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructurename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructure",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "reestrid",
                    type = "unsigned",
                    is_nullable = false,
                },
                {
                    name = "reestrstatus",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleregno",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_to",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_from",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "reestrid",
                            type = "unsigned",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "reestrid",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
        vehicle_history = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin2",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclechassisnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclereleaseyear",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoctypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdocissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoccomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptstypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptscomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodycolor",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrand",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrandmodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodynum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecost",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegasequip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleproducername",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegrossmass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclesteeringwheeltypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclekpptype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletransmissiontype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecategory",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypeunit",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleecoclass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclespecfuncname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenclosedvolume",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginenum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepower",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepowerkw",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginetype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdrestrictiondate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvalnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaldate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaltype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "utilizationfeename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocissue",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocrestriction",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerorgname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerinn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerogrn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerkpp",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressindex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdepartmentname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationattorney",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationlising",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdertypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsnils",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressdistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressbuilding",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructureid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructurename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructure",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "reestrid",
                    type = "unsigned",
                    is_nullable = false,
                },
                {
                    name = "reestrstatus",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleregno",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_to",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_from",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "reestrid",
                            type = "unsigned",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "reestrid",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        }
    }
}

res, err = api:call("set_schema", { yaml.encode(config) })
print("table was created")

fiber.sleep(3)
print("loading data was started")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

storage1:eval("return box.space.vehicle_actual:truncate();")
storage1:eval("return box.space.vehicle_history:truncate();")
storage2:eval("return box.space.vehicle_actual:truncate();")
storage2:eval("return box.space.vehicle_history:truncate();")

for i = 1, record_count, 1 do
    api:call("insert_record", {
        "vehicle_actual",
        {
            id = i,
            reestrid = i,
            vehicleguid = "1",
            reestrstatus = "1",
            vehicleregno = "1",
            vehiclevin = "1",
            vehiclevin2 = "1",
            vehiclechassisnum = "1",
            vehiclereleaseyear = "1",
            operationregdoctypename = "1",
            operationregdoc = "1",
            operationregdocissuedate = "1",
            operationregdoccomments = "1",
            vehicleptstypename = "1",
            vehicleptsnum = "1",
            vehicleptsissuedate = "1",
            vehicleptsissuer = "1",
            vehicleptscomments = "1",
            vehiclebodycolor = "1",
            vehiclebrand = "1",
            vehiclemodel = "1",
            vehiclebrandmodel = "1",
            vehiclebodynum = "1",
            vehiclecost = "1",
            vehiclegasequip = "1",
            vehicleproducername = "1",
            vehiclegrossmass = "1",
            vehiclemass = "1",
            vehiclesteeringwheeltypeid = "1",
            vehiclekpptype = "1",
            vehicletransmissiontype = "1",
            vehicletypename = "1",
            vehiclecategory = "1",
            vehicletypeunit = "1",
            vehicleecoclass = "1",
            vehiclespecfuncname = "1",
            vehicleenclosedvolume = "1",
            vehicleenginemodel = "1",
            vehicleenginenum = "1",
            vehicleenginepower = "1",
            vehicleenginepowerkw = "1",
            vehicleenginetype = "1",
            holdrestrictiondate = "1",
            approvalnum = "1",
            approvaldate = "1",
            approvaltype = "1",
            utilizationfeename = "1",
            customsdoc = "1",
            customsdocdate = "1",
            customsdocissue = "1",
            customsdocrestriction = "1",
            customscountryremovalid = "1",
            customscountryremovalname = "1",
            ownerorgname = "1",
            ownerinn = "1",
            ownerogrn = "1",
            ownerkpp = "1",
            ownerpersonlastname = "1",
            ownerpersonfirstname = "1",
            ownerpersonmiddlename = "1",
            ownerpersonbirthdate = "1",
            ownerbirthplace = "1",
            ownerpersonogrnip = "1",
            owneraddressindex = "1",
            owneraddressmundistrict = "1",
            owneraddresssettlement = "1",
            owneraddressstreet = "1",
            ownerpersoninn = "1",
            ownerpersondoccode = "1",
            ownerpersondocnum = "1",
            ownerpersondocdate = "1",
            operationname = "1",
            operationdate = "1",
            operationdepartmentname = "1",
            operationattorney = "1",
            operationlising = "1",
            holdertypeid = "1",
            holderpersondoccode = "1",
            holderpersondocnum = "1",
            holderpersondocdate = "1",
            holderpersondocissuer = "1",
            holderpersonlastname = "1",
            holderpersonfirstname = "1",
            holderpersonmiddlename = "1",
            holderpersonbirthdate = "1",
            holderpersonbirthregionid = "1",
            holderpersonsex = "1",
            holderpersonbirthplace = "1",
            holderpersoninn = "1",
            holderpersonsnils = "1",
            holderpersonogrnip = "1",
            holderaddressguid = "1",
            holderaddressregionid = "1",
            holderaddressregionname = "1",
            holderaddressdistrict = "1",
            holderaddressmundistrict = "1",
            holderaddresssettlement = "1",
            holderaddressstreet = "1",
            holderaddressbuilding = "1",
            holderaddressstructureid = "1",
            holderaddressstructurename = "1",
            holderaddressstructure = "1",
            sys_from = 332,
            sys_to = 332,
            sys_op = 0
        }
    })

    api:call("insert_record", {
        "vehicle_history",
        {
            id = i,
            reestrid = i,
            vehicleguid = "1",
            reestrstatus = "1",
            vehicleregno = "1",
            vehiclevin = "1",
            vehiclevin2 = "1",
            vehiclechassisnum = "1",
            vehiclereleaseyear = "1",
            operationregdoctypename = "1",
            operationregdoc = "1",
            operationregdocissuedate = "1",
            operationregdoccomments = "1",
            vehicleptstypename = "1",
            vehicleptsnum = "1",
            vehicleptsissuedate = "1",
            vehicleptsissuer = "1",
            vehicleptscomments = "1",
            vehiclebodycolor = "1",
            vehiclebrand = "1",
            vehiclemodel = "1",
            vehiclebrandmodel = "1",
            vehiclebodynum = "1",
            vehiclecost = "1",
            vehiclegasequip = "1",
            vehicleproducername = "1",
            vehiclegrossmass = "1",
            vehiclemass = "1",
            vehiclesteeringwheeltypeid = "1",
            vehiclekpptype = "1",
            vehicletransmissiontype = "1",
            vehicletypename = "1",
            vehiclecategory = "1",
            vehicletypeunit = "1",
            vehicleecoclass = "1",
            vehiclespecfuncname = "1",
            vehicleenclosedvolume = "1",
            vehicleenginemodel = "1",
            vehicleenginenum = "1",
            vehicleenginepower = "1",
            vehicleenginepowerkw = "1",
            vehicleenginetype = "1",
            holdrestrictiondate = "1",
            approvalnum = "1",
            approvaldate = "1",
            approvaltype = "1",
            utilizationfeename = "1",
            customsdoc = "1",
            customsdocdate = "1",
            customsdocissue = "1",
            customsdocrestriction = "1",
            customscountryremovalid = "1",
            customscountryremovalname = "1",
            ownerorgname = "1",
            ownerinn = "1",
            ownerogrn = "1",
            ownerkpp = "1",
            ownerpersonlastname = "1",
            ownerpersonfirstname = "1",
            ownerpersonmiddlename = "1",
            ownerpersonbirthdate = "1",
            ownerbirthplace = "1",
            ownerpersonogrnip = "1",
            owneraddressindex = "1",
            owneraddressmundistrict = "1",
            owneraddresssettlement = "1",
            owneraddressstreet = "1",
            ownerpersoninn = "1",
            ownerpersondoccode = "1",
            ownerpersondocnum = "1",
            ownerpersondocdate = "1",
            operationname = "1",
            operationdate = "1",
            operationdepartmentname = "1",
            operationattorney = "1",
            operationlising = "1",
            holdertypeid = "1",
            holderpersondoccode = "1",
            holderpersondocnum = "1",
            holderpersondocdate = "1",
            holderpersondocissuer = "1",
            holderpersonlastname = "1",
            holderpersonfirstname = "1",
            holderpersonmiddlename = "1",
            holderpersonbirthdate = "1",
            holderpersonbirthregionid = "1",
            holderpersonsex = "1",
            holderpersonbirthplace = "1",
            holderpersoninn = "1",
            holderpersonsnils = "1",
            holderpersonogrnip = "1",
            holderaddressguid = "1",
            holderaddressregionid = "1",
            holderaddressregionname = "1",
            holderaddressdistrict = "1",
            holderaddressmundistrict = "1",
            holderaddresssettlement = "1",
            holderaddressstreet = "1",
            holderaddressbuilding = "1",
            holderaddressstructureid = "1",
            holderaddressstructurename = "1",
            holderaddressstructure = "1",
            sys_from = 332,
            sys_to = 332,
            sys_op = 0
        }
    })
end
api:close()

print("data was loaded")
fiber.sleep(3)

print(
        string.format(
                "Storage 1 has %d actual and %d history records",
                storage1:eval("return box.space.vehicle_actual:count();"),
                storage1:eval("return box.space.vehicle_history:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d actual and %d history records",
                storage2:eval("return box.space.vehicle_actual:count();"),
                storage2:eval("return box.space.vehicle_history:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))