include config.mk

lib_dir = $(build_dir)/lib
bin_dir = $(build_dir)/bin

phase1 := $(build_dir)/phase1

# library dependencies
lib_storage := $(lib_dir)/storage
lib_access  := $(lib_dir)/access
lib_helper  := $(lib_dir)/helper
lib_io      := $(lib_dir)/io
lib_testing := $(lib_dir)/testing
lib_net     := $(lib_dir)/net
lib_layouter:= $(lib_dir)/layouter
lib_taskscheduler := $(lib_dir)/taskscheduler

# third party dependencies
json		:= $(build_dir)/jsoncpp
ext_gtest	:= $(build_dir)/gtest
lib_ebb		:= $(lib_dir)/ebb
lib_ftprinter	:= $(lib_dir)/ftprinter
lib_backward	:= $(bin_dir)/backward

# a list of all libraries
libraries := $(json) $(lib_helper) $(lib_storage) $(lib_access) $(lib_io) $(lib_testing) $(lib_net) $(lib_layouter) $(lib_ebb) $(lib_memory) $(ext_gtest) $(lib_taskscheduler) $(lib_ftprinter) $(lib_backward)

# binary dependencies
# - test suites
unit_tests_helper	:= $(bin_dir)/units_helper
unit_tests_io 		:= $(bin_dir)/units_io
unit_tests_storage 	:= $(bin_dir)/units_storage
unit_tests_nvm	 	:= $(bin_dir)/units_nvm
unit_tests_access 	:= $(bin_dir)/units_access
unit_tests_taskscheduler	:=	$(bin_dir)/units_taskscheduler
unit_tests_layouter 	:= $(bin_dir)/units_layouter
unit_tests_net	        := $(bin_dir)/units_net
perf_regression		:= $(bin_dir)/perf_regression
perf_datagen            := $(bin_dir)/perf_datagen
test_relation_eq	:= $(bin_dir)/test_relation_eq

basic_test_suites := $(unit_tests_helper) $(unit_tests_io) $(unit_tests_storage) $(unit_tests_nvm) $(unit_tests_layouter) $(unit_tests_access) $(unit_tests_taskscheduler) $(unit_tests_memory) $(unit_tests_net)
aux_test_suites := $(test_relation_eq)

regression_suite := $(perf_regression)
regression_datagen := $(perf_datagen)

all_test_suites := $(complex_test_suites) $(basic_test_suites) $(aux_test_suites)

all_test_binaries := $(subst bin/,,$(all_test_suites))
basic_test_binaries := $(subst bin/,,$(basic_test_suites))
regression_binaries := $(subst bin/,,$(regression_suite))
datagen_binaries    := $(subst bin/,,$(regression_datagen))

# - other binaries
server_hyrise 		:= $(bin_dir)/hyrise
bin_dummy := $(bin_dir)/dummy

binaries :=  $(server_hyrise) $(bin_dummy)
# list all build targets

tgts :=  $(libraries) $(binaries) $(all_test_suites) $(regression_suite) $(regression_datagen) $(phase1)

.PHONY: all $(tgts) tags test test_basic hudson_build hudson_test $(all_test_binaries) doxygen docs .allin

all: $(tgts) .allin

.allin:
	g++-4.8 -o ./build/hyrise_server_allin ./build/lib/access/HashJoinProbe.cpp.o ./build/lib/access/PosUpdateIncrementScan.cpp.o ./build/lib/access/ProjectionScan.cpp.o ./build/lib/access/TableScan.cpp.o ./build/lib/access/expressions/ExpressionRegistration.cpp.o ./build/lib/access/expressions/GenericExpressions.cpp.o ./build/lib/access/expressions/pred_PredicateBuilder.cpp.o ./build/lib/access/expressions/ExampleExpression.cpp.o ./build/lib/access/expressions/pred_buildExpression.cpp.o ./build/lib/access/expressions/expression_types.cpp.o ./build/lib/access/JoinScan.cpp.o ./build/lib/access/tx/Commit.cpp.o ./build/lib/access/tx/ValidatePositions.cpp.o ./build/lib/access/tx/Rollback.cpp.o ./build/lib/access/system/ResponseTask.cpp.o ./build/lib/access/system/QueryParser.cpp.o ./build/lib/access/system/QueryTransformationEngine.cpp.o ./build/lib/access/system/ProfilingOperations.cpp.o ./build/lib/access/system/ParallelizablePlanOperation.cpp.o ./build/lib/access/system/RequestParseTask.cpp.o ./build/lib/access/system/PlanOperation.cpp.o ./build/lib/access/system/SettingsOperation.cpp.o ./build/lib/access/system/OperationData.cpp.o ./build/lib/access/Delete.cpp.o ./build/lib/access/IntersectPositions.cpp.o ./build/lib/access/SmallestTableScan.cpp.o ./build/lib/access/CreateIndex.cpp.o ./build/lib/access/OperationListingHandler.cpp.o ./build/lib/access/GroupByScan.cpp.o ./build/lib/access/UnionAll.cpp.o ./build/lib/access/Distinct.cpp.o ./build/lib/access/MultiplyRefField.cpp.o ./build/lib/access/SimpleRawTableScan.cpp.o ./build/lib/access/RadixJoin.cpp.o ./build/lib/access/Layouter.cpp.o ./build/lib/access/UpdateScan.cpp.o ./build/lib/access/radixjoin/NestedLoopEquiJoin.cpp.o ./build/lib/access/radixjoin/RadixCluster.cpp.o ./build/lib/access/radixjoin/RadixJoinTransformation.cpp.o ./build/lib/access/radixjoin/Histogram.cpp.o ./build/lib/access/radixjoin/PrefixSum.cpp.o ./build/lib/access/InsertScan.cpp.o ./build/lib/access/storage/SetTable.cpp.o ./build/lib/access/storage/TableLoad.cpp.o ./build/lib/access/storage/LoadFile.cpp.o ./build/lib/access/storage/UnloadAll.cpp.o ./build/lib/access/storage/TableIO.cpp.o ./build/lib/access/storage/GetTable.cpp.o ./build/lib/access/storage/TableUnload.cpp.o ./build/lib/access/storage/ReplaceTable.cpp.o ./build/lib/access/storage/MySQLTableLoad.cpp.o ./build/lib/access/storage/JsonTable.cpp.o ./build/lib/access/SortScan.cpp.o ./build/lib/access/MergeTable.cpp.o ./build/lib/access/NoOp.cpp.o ./build/lib/access/LayoutTableLoad.cpp.o ./build/lib/access/HashBuild.cpp.o ./build/lib/access/IndexScan.cpp.o ./build/lib/access/MaterializingScan.cpp.o ./build/lib/access/AggregateFunctions.cpp.o ./build/lib/access/LayoutTable.cpp.o ./build/lib/access/Barrier.cpp.o ./build/lib/access/UnionScan.cpp.o ./build/lib/access/SimpleTableScan.cpp.o ./build/lib/access/json_converters.cpp.o ./build/lib/access/MergeHashTables.cpp.o ./build/lib/access/PosUpdateScan.cpp.o ./build/lib/access/MetaData.cpp.o ./build/lib/access/ScriptOperation.cpp.o ./build/lib/access/ExpressionScan.cpp.o ./build/lib/ebb/ebb_request_parser.c.o ./build/lib/ebb/ebb.c.o ./build/lib/ftprinter/BufferedFTPrinter.cpp.o ./build/lib/ftprinter/PrintFormat.cpp.o ./build/lib/ftprinter/FTPrinter.cpp.o ./build/lib/storage/RawTable.cpp.o ./build/lib/storage/TableMerger.cpp.o ./build/lib/storage/GroupValue.cpp.o ./build/lib/storage/AbstractTable.cpp.o ./build/lib/storage/TableGenerator.cpp.o ./build/lib/storage/ColumnMetadata.cpp.o ./build/lib/storage/PrettyPrinter.cpp.o ./build/lib/storage/AbstractResource.cpp.o ./build/lib/storage/SimpleStoreMerger.cpp.o ./build/lib/storage/TableFactory.cpp.o ./build/lib/storage/SequentialHeapMerger.cpp.o ./build/lib/storage/Serial.cpp.o ./build/lib/storage/Store.cpp.o ./build/lib/storage/HorizontalTable.cpp.o ./build/lib/storage/TableUtils.cpp.o ./build/lib/storage/AbstractAttributeVector.cpp.o ./build/lib/storage/storage_types_helper.cpp.o ./build/lib/storage/SimpleStore.cpp.o ./build/lib/storage/TableDiff.cpp.o ./build/lib/storage/TableRangeView.cpp.o ./build/lib/storage/MutableVerticalTable.cpp.o ./build/lib/storage/csb_tree.cpp.o ./build/lib/storage/PointerCalculator.cpp.o ./build/lib/storage/TableBuilder.cpp.o ./build/lib/storage/AbstractIndex.cpp.o ./build/lib/storage/Table.cpp.o ./build/lib/storage/HashTable.cpp.o ./build/lib/io/AbstractLoader.cpp.o ./build/lib/io/RawTableLoader.cpp.o ./build/lib/io/StorageManager.cpp.o ./build/lib/io/BufferedLogger.cpp.o ./build/lib/io/NVManager.cpp.o ./build/lib/io/Loader.cpp.o ./build/lib/io/SimpleLogger.cpp.o ./build/lib/io/TableDump.cpp.o ./build/lib/io/GenericCSV.cpp.o ./build/lib/io/ResourceManager.cpp.o ./build/lib/io/EmptyLoader.cpp.o ./build/lib/io/CSVLoader.cpp.o ./build/lib/io/VectorLoader.cpp.o ./build/lib/io/MetadataCreation.cpp.o ./build/lib/io/TransactionManager.cpp.o ./build/lib/io/MySQLLoader.cpp.o ./build/lib/io/StringLoader.cpp.o ./build/lib/io/TransactionRouteHandler.cpp.o ./build/lib/io/MPassCSVLoader.cpp.o ./build/lib/io/shortcuts.cpp.o ./build/lib/helper/demangle.cpp.o ./build/lib/helper/HwlocHelper.cpp.o ./build/lib/helper/Settings.cpp.o ./build/lib/helper/sha1.cpp.o ./build/lib/helper/fs.cpp.o ./build/lib/helper/prefetching.cpp.o ./build/lib/helper/ConfigFile.cpp.o ./build/lib/helper/epoch.cpp.o ./build/lib/helper/HttpHelper.cpp.o ./build/lib/helper/hash.cpp.o ./build/lib/helper/Environment.cpp.o ./build/lib/helper/unique_id.cpp.o ./build/lib/net/AbstractConnection.cpp.o ./build/lib/net/Router.cpp.o ./build/lib/net/StaticRequestHandler.cpp.o ./build/lib/net/AsyncConnection.cpp.o ./build/lib/net/RouteRequestHandler.cpp.o ./build/lib/taskscheduler/WSCoreBoundPriorityQueuesScheduler.cpp.o ./build/lib/taskscheduler/CentralPriorityScheduler.cpp.o ./build/lib/taskscheduler/CoreBoundQueuesScheduler.cpp.o ./build/lib/taskscheduler/Task.cpp.o ./build/lib/taskscheduler/ThreadPerTaskScheduler.cpp.o ./build/lib/taskscheduler/WSCoreBoundQueuesScheduler.cpp.o ./build/lib/taskscheduler/WSCoreBoundPriorityQueue.cpp.o ./build/lib/taskscheduler/CoreBoundPriorityQueue.cpp.o ./build/lib/taskscheduler/AbstractCoreBoundQueuesScheduler.cpp.o ./build/lib/taskscheduler/CoreBoundPriorityQueuesScheduler.cpp.o ./build/lib/taskscheduler/WSCoreBoundQueue.cpp.o ./build/lib/taskscheduler/CentralScheduler.cpp.o ./build/lib/taskscheduler/CoreBoundQueue.cpp.o ./build/lib/taskscheduler/AbstractCoreBoundQueue.cpp.o ./build/lib/layouter/layout_utils.cpp.o ./build/lib/layouter/incremental.cpp.o ./build/lib/layouter/base.cpp.o ./build/bin/hyrise/main.cpp.o -lnuma -ldl -Wl,-no-as-needed -g3 -lmysqlclient -llog4cxx -lpthread -lboost_system -lbackward-hyr -lboost_system -lboost_filesystem -ltbb -lcsv -lmetis -lrt -ljson -lboost_thread -lev -lebb -lboost_program_options -lhwloc -L/usr/local/lib -L/home/David.Schwalb/hyrise/build -L/usr/lib64
	rm ./build/hyrise_server
	mv ./build/hyrise_server_allin ./build/hyrise_server

$(tgts):
	@echo 'DIR $@'
	@$(MAKE) -$(MAKEFLAGS) --directory=$@ --no-print-directory

# dependencies betweeen binaries and libraries

$(libraries): $(phase1)
$(lib_ebb):
$(lib_helper):
$(lib_taskscheduler): $(lib_helper)
$(lib_storage): $(lib_helper) $(lib_ftprinter) $(ext_gtest) $(lib_ftprinter)
$(lib_io): $(lib_storage) $(lib_helper) $(lib_net)
$(lib_access): $(lib_storage) $(lib_helper) $(lib_io) $(lib_layouter) $(json) $(lib_taskscheduler) $(lib_net)
$(lib_testing): $(ext_gtest) $(lib_storage) $(lib_taskscheduler) $(lib_access)
$(lib_net): $(lib_helper) $(json) $(lib_taskscheduler) $(lib_ebb)

$(server_hyrise): $(libraries)
$(unit_tests_helper): $(libraries)
$(unit_tests_io): $(libraries)
$(unit_tests_storage): $(libraries)
$(unit_tests_nvm): $(libraries)
$(unit_tests_net): $(libraries)
$(unit_tests_access): $(libraries)
$(unit_tests_taskscheduler): $(libraries)
$(unit_tests_layouter): $(libraries)
$(unit_tests_memory): $(libraries)
$(all_test_suites): $(libraries)
$(perf_regression): $(libraries)
$(perf_datagen): $(libraries)
$(test_relation_eq): $(libraries)

$(binaries): $(libraries)
$(all_test_suites): $(libraries)

# --- Hack: To make the execution of test tasks sensible to errors when
# running taks `test_basic` or `test`, we set an env variable so the following
# rule can be initialized correctly
ERROR_MODE =
ifeq "$(MAKECMDGOALS)" "test_basic"
	ERROR_MODE := stop_on_error
endif

ifeq "$(MAKECMDGOALS)" "test"
	ERROR_MODE := stop_on_error
endif
# end of hack

$(all_test_binaries): $(all_test_suites)
ifeq ($(ERROR_MODE), stop_on_error)
	$@ $(unit_test_params)
else
	-$@ $(unit_test_params)
# Just make sure we don't have linking errors
	$@ --gtest_list_tests > /dev/null
endif

python_test:
	python tools/test_server.py

basic_test_targets := $(basic_test_binaries) python_test
all_test_targets := $(all_test_binaries) python_test

# Test invocation rules
test: unit_test_params = --minimal
test: $(basic_test_targets)
test_verbose: $(basic_test_targets)
test_all: $(all_test_targets)

include makefiles/*.mk

clean_dependencies:
	@find . -type f -name '*.d' -delete

# Clean up: for every target tgt in $(tgts), we create a rule $(tgt)_clean
clean_targets :=
define CLEAN_TGT
clean_targets += $(1)_clean
$(1)_clean:
	@$(MAKE) clean --directory=$(1) --no-print-directory
endef
$(foreach tgt,$(tgts),$(eval $(call CLEAN_TGT,$(tgt))))

clean: $(clean_targets) clean_dependencies

tags:
	find `pwd`/src -type f | egrep 'c$$|cc$$|cpp$$|h$$|hpp$$' | egrep -v '\.#.*'| etags -

benchmark_data:
	mkdir -p benchmark_data
	$(build_dir)/perf_datagen -w 10 -d benchmark_data --hyrise

codespeed: $(all) benchmark_data
	@echo ' '
	@echo 'Executing performance regression...'
	@echo ' '
	HYRISE_DB_PATH=$(IMH_PROJECT_PATH)/benchmark_data $(build_dir)/perf_regression --gtest_catch_exceptions=0 --gtest_output=xml:benchmark.xml

duplicates:
	-java -jar third_party/simian/simian-2.3.33.jar -excludes="**/units_*/*" -formatter=xml:simian.xml "src/**.cpp" "src/**.h" "src/**.hpp"

pushdocs: doxygen
	cd docs
	$(MAKE) clean
	mkdir -p _build/html
	cd _build
	git clone -b gh-pages git@github.com:hyrise/hyrise.git html
	cd ..
	make html
	cd _build/html
	git add .
	git commit -am "Updating docs"
	git push origin gh-pages
