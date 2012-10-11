%%%-------------------------------------------------------------------
%%% @author Tristan Sloughter <>
%%% @copyright (C) 2010, 2012, Tristan Sloughter
%%% @doc
%%%
%%% @end
%%% Created : 14 Feb 2010 by Tristan Sloughter <>
%%%-------------------------------------------------------------------
-module(erlastic_search).
-compile([export_all]).

-include_lib("erlastic_search/include/erlastic_search.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Takes the name of an index to create and sends the request to
%% Elastic Search, the default settings on localhost.
%%
%% @spec create_index(Index) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
create_index(Index) ->
    create_index(Index, []).

create_index(Index, Json) ->
    erls_resource:put(#erls_params{}, Index, [], [], Json, []).


%%--------------------------------------------------------------------
%% @doc Set mappings for index and type.
%% @spec set_index_mapping(Index, Type, Mappings) -> {ok, Data} | {error, Error}
%%--------------------------------------------------------------------
set_index_mapping(Index, Type, MappingsMochijson) when is_tuple(MappingsMochijson) ->
    MappingsJson = erls_utils:json_encode(MappingsMochijson),
    set_index_mapping(Index, Type, MappingsJson);
set_index_mapping(Index, Type, MappingsJson) ->
    Path = filename:join([Index, Type, "_mapping"]),
    erls_resource:put(#erls_params{}, Path, [], [], MappingsJson, []).


%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string and passes to the
%% default server. Elastic Search provides the doc with an id.
%%
%% @spec index(Index, Type, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc(Index, Type, Doc) ->
    index_doc(Index, Type, Doc, []).

index_doc(Index, Type, Doc, Qs) when is_tuple(Doc) ->
    Json = erls_utils:json_encode(Doc),
    index_doc(Index, Type, Json, Qs);
index_doc(Index, Type, Json, Qs) ->
    ReqPath = filename:join(Index, Type),
    erls_resource:post(#erls_params{}, ReqPath, [], Qs, Json, []).


%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string after adding the _id field
%% and passes to the default server.
%%
%% @spec index(Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc_with_id(Index, Type, Id, Doc) ->
    index_doc_with_id(Index, Type, Id, Doc, []).

index_doc_with_id(Index, Type, Id, Doc, Qs) when is_tuple(Doc) ->
    Json = erls_utils:json_encode(Doc),
    index_doc_with_id(Index, Type, Id, Json, Qs);
index_doc_with_id(Index, Type, Id, Json, Qs) ->
    Id1 = mochiweb_util:quote_plus(Id),
    Path = filename:join([Index, Type, Id1]),
    erls_resource:post(#erls_params{}, Path, [], Qs, Json, []).


to_bin(L) when is_list(L)   -> list_to_binary(L);
to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A)   -> to_bin(atom_to_list(A)).


%% Documents is [ {Index, Type, Id, Json}, ... ]
bulk_index_docs(Params, IndexTypeIdJsonTuples) ->
    Body = lists:map(fun({Index, Type, Id, Json}) ->
         Header = erls_utils:json_encode({struct, [
                                              {<<"index">>, [ {struct, [
                                                                        {<<"_index">>, to_bin(Index)},
                                                                        {<<"_type">>, to_bin(Type)},
                                                                        {<<"_id">>, to_bin(Id)}
                                                                       ]}]}]}),
                             [
                              Header,
                              <<"\n">>,
                              Json,
                              <<"\n">>
                             ]
                     end, IndexTypeIdJsonTuples),
    erls_resource:post(Params, "/_bulk", [], [], Body, []).


search(Index, Query) ->
    search(#erls_params{}, Index, "", Query, []).

search(Params, Index, Query) when is_record(Params, erls_params) ->
    search(Params, Index, "", Query, []);

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a query as "key:value" and sends
%% it to the default Elastic Search server on localhost:9100
%%
%% @spec search(Index, Type, Query) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
search(Index, Type, Query) ->
    search(#erls_params{}, Index, Type, Query, []). 

search_limit(Index, Type, Query, Limit) when is_integer(Limit) ->
    search(#erls_params{}, Index, Type, Query, [{"size", integer_to_list(Limit)}]). 
%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a query as "key:value" and sends
%% it to the Elastic Search server specified in Params.
%%
%% @spec search(Params, Index, Type, Query) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
search(Params, Index=[H|_T], Type=[H2|_T2], Query, Opts) when not is_list(H), is_list(H2) ->
    search(Params, [Index], Type, Query, Opts);
search(Params, Index=[H|_T], Type=[H2|_T2], Query, Opts) when is_list(H), not is_list(H2) ->
    search(Params, Index, [Type], Query, Opts);
search(Params, Index=[H|_T], Type=[H2|_T2], Query, Opts) when not is_list(H), not is_list(H2) ->
    search(Params, [Index], [Type], Query, Opts);
search(Params, Index, Type, Query, Opts) ->
    Path = filename:join([Index, Type, "_search"]),
    erls_resource:get(Params, Path, [], [{"q", Query}|Opts], []).


%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a query mochijson struct {struct, ...} and sends
%% it to the Elastic Search server specified in request body.
%%
%% @spec search(Params, Index, Type, Query) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
search_mochijson(Index, Type, QueryMochijson) ->
    search_mochijson(Index, Type, QueryMochijson, []).
search_mochijson(Index, Type, QueryMochijson, Qs) ->
    Json = erls_utils:json_encode(QueryMochijson),
    search_json(#erls_params{}, Index, Type, Json, Qs).

search_json(Params, Index, Type, Json) ->
    search_json(Params, Index, Type, Json, []).
search_json(Params, Index, Type, Json, Qs) ->
    Path = Index ++ [$/ | Type] ++ "/_search",
    erls_resource:get(Params, Path, [], Qs, Json, []).


%% To create empty-body search-request:
match_all(Index, Type, Qs) ->
    ReqBody = [],
    Path = Index ++ [$/ | Type] ++ "/_search",
    erls_resource:get(#erls_params{}, Path, [], Qs, ReqBody, []).


%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a doc id and sends
%% it to the default Elastic Search server on localhost:9100
%%
%% @spec index(Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_doc(Index, Type, Id) ->
    get_doc(Index, Type, Id, []).

get_doc(Index, Type, Id, Qs) ->
    Id1 = mochiweb_util:quote_plus(Id),
    ReqPath = filename:join([Index, Type, Id1]),
    erls_resource:get(#erls_params{}, ReqPath, [], Qs, []).


%%--------------------------------------------------------------------
%% @doc A multiget: get plenty of documents at once.
%%--------------------------------------------------------------------
multiget_mochijson(Index, Type, Mochijson) ->
    multiget_mochijson(Index, Type, Mochijson, []).
multiget_mochijson(Index, Type, Mochijson, Qs) ->
    ReqPath = Index ++ [$/|Type] ++ "/_mget",
    ReqBody = erls_utils:json_encode(Mochijson),
    erls_resource:get(#erls_params{}, ReqPath, [], Qs, ReqBody, []).


flush_index(Index) ->
    flush_index(Index, false).
flush_index(Index, IsRefresh) ->
    Qs = case IsRefresh of
	false -> [];
	true  -> [{"refresh", "true"}]
    end,
    erls_resource:post(#erls_params{}, Index ++ "/_flush", [], Qs, [], []).

flush_all() ->
    refresh_all().

flush_all(Params) ->
    erls_resource:post(Params, "_flush", [], [], [], []).


refresh_index(Index) ->
    erls_resource:post(#erls_params{}, Index ++ "/_refresh", [], [], [], []).


refresh_all() ->
    erls_resource:post(#erls_params{}, "_refresh", [], [], [], []).


delete_doc(Index, Type, Id) ->
    delete_doc(Index, Type, Id, []).
delete_doc(Index, Type, Id, Qs) ->
    Id1 = mochiweb_util:quote_plus(Id),
    Path = Index ++ [$/ | Type] ++ [$/ | Id1],
    erls_resource:delete(#erls_params{}, Path, [], Qs, []).


delete_doc_by_query(Index, Type, Query) ->
    ReqPath = filename:join([Index, Type]),
    erls_resource:delete(#erls_params{}, ReqPath, [], [{"q", Query}], []).


optimize_index(Index) ->
    erls_resource:post(#erls_params{}, Index ++ "/_optimize", [], [], [], []).


delete_index(Index) ->
    erls_resource:delete(#erls_params{}, Index, [], [], []).


set_index_settings(Index, Settings) ->
    SettingsJson = erls_utils:json_encode(Settings),
    erls_resource:put(#erls_params{}, Index ++ "/_settings", [], [], SettingsJson, []). 

