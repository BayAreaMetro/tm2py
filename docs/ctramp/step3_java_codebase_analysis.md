
# Travel Model Two Java Codebase Analysis
**Generated:** 2025-09-25 17:03:07
**Purpose:** CT-RAMP Java implementation structure analysis

## Executive Summary

- **Total Java files:** 1299
- **Classes analyzed:** 807
- **Packages identified:** 147
- **UEC processors:** 170
- **Model components:** 167

## Package Structure

### com.pb.common.calculator
**Files:** 4

- **LinkCalculator** (228 lines)
  - Patterns: UEC_PROCESSING, DATA_MANAGEMENT
- **LinkFunction** (191 lines)
  - Patterns: UEC_PROCESSING, DATA_MANAGEMENT
- **MatrixCalculator** (241 lines)
  - Patterns: UEC_PROCESSING
- **implementation** (31 lines)
  - Patterns: UEC_PROCESSING

### com.pb.common.calculator2
**Files:** 14

- **Constants** (63 lines)
  - Patterns: UEC_PROCESSING
- **DataEntry** (51 lines)
  - Patterns: UEC_PROCESSING
- **Expression** (979 lines)
  - Patterns: UEC_PROCESSING
- **ExpressionFlags** (33 lines)
  - Patterns: UEC_PROCESSING
- **ExpressionIndex** (66 lines)
  - Patterns: UEC_PROCESSING
  - ... and 9 more files

### com.pb.common.calculator2.tests
**Files:** 5

- **DMU** (77 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **ExpressionTest** (197 lines)
  - Patterns: UEC_PROCESSING
- **LinkFunctionTest** (109 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR, DATA_MANAGEMENT
- **MatrixCalculatorTest** (115 lines)
  - Patterns: UEC_PROCESSING
- **UECTest** (111 lines)
  - Patterns: UEC_PROCESSING

### com.pb.common.datafile
**Files:** 26

- **BaseDataFile** (446 lines)
  - Patterns: DATA_MANAGEMENT
- **from** (715 lines)
  - Patterns: DATA_MANAGEMENT
- **D211FileReader** (290 lines)
  - Patterns: MODEL_MANDATORY_TOUR, DATA_MANAGEMENT
- **D231FileReader** (129 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **DataFile** (232 lines)
  - Patterns: DATA_MANAGEMENT
  - ... and 21 more files

### com.pb.common.datafile.tests
**Files:** 8

- **BinaryFileTest** (105 lines)
  - Patterns: MODEL_STOP_FREQUENCY, DATA_MANAGEMENT
- **CSVFileReaderTest** (83 lines)
  - Patterns: MODEL_STOP_FREQUENCY, DATA_MANAGEMENT
- **CSVFileWriterTest** (64 lines)
  - Patterns: DATA_MANAGEMENT
- **DataFileTest** (85 lines)
  - Patterns: DATA_MANAGEMENT
- **DiskObjectArrayTest** (193 lines)
  - ... and 3 more files

### com.pb.common.datastore
**Files:** 2

- **DataStoreManager** (55 lines)
  - Patterns: DATA_MANAGEMENT
- **JDBCManager** (43 lines)

### com.pb.common.emme2.io
**Files:** 3

- **Emme2DataBank** (880 lines)
- **Emme2FileParameters** (38 lines)
- **GlobalDatabankParameters** (41 lines)

### com.pb.common.env
**Files:** 7

- **Execute** (658 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **ExecuteStreamHandler** (61 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **Os** (199 lines)
- **ProcessDestroyer** (89 lines)
- **ProcessEnvironment** (71 lines)
  - ... and 2 more files

### com.pb.common.env.tests
**Files:** 1

- **TestProcessEnvironment** (49 lines)

### com.pb.common.graph
**Files:** 5

- **DataPoint** (52 lines)
- **GraphData** (31 lines)
- **MapValue** (43 lines)
- **ThematicGraphData** (38 lines)
- **XYGraphData** (55 lines)

### com.pb.common.grid
**Files:** 6

- **ConvertASCIItoGridFile** (163 lines)
- **ConvertGridFiletoASCII** (74 lines)
- **GridDataBuffer** (165 lines)
- **GridFile** (407 lines)
- **GridOperations** (104 lines)
  - Patterns: DATA_MANAGEMENT
  - ... and 1 more files

### com.pb.common.grid.tests
**Files:** 2

- **CreateMemoryMap** (68 lines)
- **GridFileTest** (397 lines)

### com.pb.common.http
**Files:** 8

- **ClassFileServerTest** (55 lines)
- **ClassRunner** (89 lines)
- **Dependency** (8 lines)
- **HttpRequestProcessor** (92 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **HttpServer** (209 lines)
  - Patterns: MODEL_MANDATORY_TOUR
  - ... and 3 more files

### com.pb.common.image
**Files:** 1

- **ImageFactory** (335 lines)

### com.pb.common.image.tests
**Files:** 1

- **SVGImageTest** (86 lines)

### com.pb.common.logging
**Files:** 1

- **LogServerTest** (32 lines)

### com.pb.common.math
**Files:** 4

- **LogExpCalculator** (75 lines)
  - Patterns: UEC_PROCESSING
- **MathNative** (40 lines)
- **replacement** (729 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **NumericalIntegrator** (19 lines)

### com.pb.common.math.tests
**Files:** 5

- **Benchmark** (234 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **JMath** (2535 lines)
- **LogExpCalculatorTest** (116 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **MersenneTwisterTest** (317 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS
- **TestNumericalIntegrator** (28 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### com.pb.common.matrix
**Files:** 53

- **AlphaToBetaInterface** (69 lines)
- **BinaryMatrixReader** (165 lines)
- **BinaryMatrixWriter** (146 lines)
- **ColumnVector** (211 lines)
- **CrowbarMatrixReader** (75 lines)
  - ... and 48 more files

### com.pb.common.matrix.tests
**Files:** 14

- **CreateZipFile** (122 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **MatrixBalancerMain** (269 lines)
- **MatrixBalancerTest** (267 lines)
- **MatrixTest** (186 lines)
  - Patterns: UEC_PROCESSING
- **NDimensionalMatrixDoubleTest** (87 lines)
  - ... and 9 more files

### com.pb.common.matrix.ui
**Files:** 4

- **MatrixDataModel** (62 lines)
- **MatrixNameDialog** (127 lines)
- **MatrixViewer** (180 lines)
- **MatrixViewerPanel** (276 lines)

### com.pb.common.matrix.util
**Files:** 2

- **MatrixUtil** (537 lines)
  - Patterns: MODEL_STOP_FREQUENCY, DATA_MANAGEMENT
- **MatrixUtilTest** (630 lines)
  - Patterns: MODEL_MANDATORY_TOUR, DATA_MANAGEMENT

### com.pb.common.model
**Files:** 10

- **ChoiceStrategy** (32 lines)
- **CompositeAlternative** (31 lines)
- **DecisionMakerAttributes** (28 lines)
- **DiscreteChoiceModel** (365 lines)
  - Patterns: UEC_PROCESSING
- **DiscreteChoiceModelHelper** (324 lines)
  - Patterns: MODEL_MANDATORY_TOUR
  - ... and 5 more files

### com.pb.common.model.tests
**Files:** 1

- **and** (172 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS

### com.pb.common.newmodel
**Files:** 5

- **UtilityExpressionCalculator** (2326 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **Alternative** (118 lines)
  - Patterns: UEC_PROCESSING
- **ConcreteAlternative** (179 lines)
  - Patterns: UEC_PROCESSING
- **LogitModel** (464 lines)
  - Patterns: UEC_PROCESSING
- **MultinomialLogitModel** (154 lines)
  - Patterns: UEC_PROCESSING

### com.pb.common.sql
**Files:** 2

- **JDBCConnection** (189 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **SQLHelper** (63 lines)

### com.pb.common.sql.tests
**Files:** 2

- **JDBCConnectionTest** (113 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **MySQLTest** (144 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### com.pb.common.summit
**Files:** 5

- **MergeSummitFiles** (235 lines)
  - Patterns: UEC_PROCESSING, MODEL_INDIVIDUAL_TOUR
- **SummitFileReader** (328 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **SummitFileWriter** (272 lines)
  - Patterns: MODEL_TOUR_TOD
- **SummitHeader** (196 lines)
  - Patterns: MODEL_TOUR_TOD
- **SummitRecordTable** (238 lines)
  - Patterns: UEC_PROCESSING, MODEL_STOP_LOCATION

### com.pb.common.ui.swing
**Files:** 10

- **ColoredCellRenderer** (45 lines)
- **DecimalFormatRenderer** (43 lines)
- **HeaderRenderer** (72 lines)
- **JScrollPaneAdjuster** (179 lines)
- **JTableRowHeaderResizer** (193 lines)
  - ... and 5 more files

### com.pb.common.util
**Files:** 20

- **contains** (39 lines)
- **BinarySearch** (162 lines)
- **BooleanLock** (143 lines)
- **catch** (146 lines)
- **DosCommand** (288 lines)
  - Patterns: DATA_MANAGEMENT
  - ... and 15 more files

### com.pb.common.util.tests
**Files:** 4

- **BooleanLockTest** (113 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **IndexMergeSortTest** (56 lines)
- **PerformanceTimerTest** (104 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **ResourceUtilTest** (46 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### com.pb.mtctm2.abm.accessibilities
**Files:** 7

- **AccessibilitiesDMU** (194 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **BestTransitPathCalculator** (1588 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **DcUtilitiesTaskJppf** (516 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **MandatoryAccessibilitiesDMU** (206 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **StoredTransitSkimData** (56 lines)
  - Patterns: UEC_PROCESSING
  - ... and 2 more files

### com.pb.mtctm2.abm.application
**Files:** 27

- **MTCTM2TourBasedModel** (314 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS
- **MTCTM2TripTables** (921 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **NodeZoneMapping** (120 lines)
  - Patterns: MODEL_MANDATORY_TOUR, DATA_MANAGEMENT
- **SandagAtWorkSubtourFrequencyDMU** (64 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS
- **SandagAutoOwnershipChoiceDMU** (120 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS
  - ... and 22 more files

### com.pb.mtctm2.abm.ctramp
**Files:** 79

- **ControlFileReader** (626 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **ChoiceModelApplication** (649 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS
- **will** (740 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **holds** (18 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS
- **AtWorkSubtourFrequencyDMU** (203 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS
  - ... and 74 more files

### com.pb.mtctm2.abm.reports
**Files:** 6

- **CreateLogsums** (405 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, MAIN_MODEL_RUNNER, DATA_MANAGEMENT
- **DataExporter** (1777 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **ModelOutputReader** (764 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **ReportBuilder** (275 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **SkimBuilder** (559 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
  - ... and 1 more files

### com.pb.mtctm2.abm.survey
**Files:** 1

- **reads** (385 lines)
  - Patterns: UEC_PROCESSING, MODEL_ACCESSIBILITY, DATA_MANAGEMENT

### com.pb.mtctm2.abm.transitcapacityrestraint
**Files:** 3

- **DeadheadModel** (345 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **ParkingCapacityRestraintModel** (1075 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT
- **chooses** (768 lines)
  - Patterns: UEC_PROCESSING, MODEL_POPULATION_SYNTHESIS, DATA_MANAGEMENT

### org.jppf
**Files:** 3

- **JPPFException** (53 lines)
- **JPPFNodeReconnectionNotification** (53 lines)
- **JPPFNodeReloadNotification** (34 lines)

### org.jppf.classloader
**Files:** 2

- **NonDelegatingClassLoader** (100 lines)
- **SaveFileAction** (95 lines)

### org.jppf.client
**Files:** 4

- **ClientConnectionHandler** (49 lines)
- **JPPFClientConnection** (60 lines)
- **JPPFResultCollector** (132 lines)
- **TestJPPFClient** (71 lines)
  - Patterns: MODEL_STOP_FREQUENCY

### org.jppf.client.concurrent
**Files:** 6

- **FutureResultCollector** (175 lines)
- **FutureResultCollectorEvent** (46 lines)
- **JPPFExecutorService** (453 lines)
  - Patterns: MODEL_INDIVIDUAL_TOUR
- **JPPFTaskFuture** (164 lines)
- **RunnableWrapper** (64 lines)
  - ... and 1 more files

### org.jppf.client.event
**Files:** 4

- **ClientConnectionStatusEvent** (64 lines)
- **ClientConnectionStatusHandler** (53 lines)
- **ClientListener** (34 lines)
- **TaskResultEvent** (79 lines)

### org.jppf.client.loadbalancer
**Files:** 2

- **ClientProportionalBundler** (83 lines)
- **TaskWrapper** (58 lines)

### org.jppf.client.taskwrapper
**Files:** 5

- **CallableTaskWrapper** (63 lines)
- **PojoTaskWrapper** (114 lines)
- **PrivilegedConstructorAction** (61 lines)
- **PrivilegedMethodAction** (67 lines)
- **RunnableTaskWrapper** (66 lines)

### org.jppf.comm.discovery
**Files:** 2

- **handle** (132 lines)
- **broadcast** (158 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.comm.recovery
**Files:** 5

- **ClientConnection** (173 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **ClientConnectionEvent** (46 lines)
- **should** (35 lines)
- **checks** (185 lines)
- **ReaperEvent** (46 lines)

### org.jppf.comm.socket
**Files:** 4

- **provides** (110 lines)
- **SocketChannelClient** (443 lines)
- **BootstrapObjectSerializer** (136 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **attempt** (199 lines)
  - Patterns: MODEL_STOP_FREQUENCY

### org.jppf.doc
**Files:** 5

- **AbstractFileFilter** (85 lines)
- **generates** (342 lines)
- **JPPFDirFilter** (79 lines)
- **JPPFFileFilter** (76 lines)
- **SamplesPHPReadmeProcessor** (150 lines)
  - Patterns: MODEL_STOP_LOCATION

### org.jppf.example.dataencryption
**Files:** 1

- **SecureKeyCipherTransform** (176 lines)
  - Patterns: MODEL_STOP_LOCATION

### org.jppf.example.dataencryption.old
**Files:** 1

- **DESCipherTransform** (113 lines)
  - Patterns: MODEL_STOP_LOCATION

### org.jppf.example.jmxlogger.test
**Files:** 2

- **LoggingRunner** (243 lines)
- **LoggingTask** (61 lines)

### org.jppf.io
**Files:** 13

- **ByteBufferInputStream** (169 lines)
- **ByteBufferOutputStream** (124 lines)
- **ChannelInputSource** (136 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **ChannelOutputDestination** (92 lines)
  - Patterns: MODEL_STOP_LOCATION
- **FileDataLocation** (369 lines)
  - Patterns: MODEL_STOP_LOCATION
  - ... and 8 more files

### org.jppf.jca.cci
**Files:** 3

- **JPPFConnectionMetaData** (72 lines)
- **JPPFInteraction** (106 lines)
- **JPPFInteractionSpec** (87 lines)

### org.jppf.jca.demo
**Files:** 2

- **DemoTask** (65 lines)
- **DurationTask** (84 lines)

### org.jppf.jca.serialization
**Files:** 2

- **JcaObjectSerializerImpl** (54 lines)
- **JcaSerializationHelperImpl** (72 lines)

### org.jppf.jca.spi
**Files:** 4

- **JPPFManagedConnection** (200 lines)
- **JPPFManagedConnectionFactory** (142 lines)
- **JPPFManagedConnectionMetaData** (88 lines)
- **initiates** (242 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.jca.work
**Files:** 3

- **JcaSocketInitializer** (92 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **run** (104 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **JPPFJcaResultCollector** (86 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.jca.work.submission
**Files:** 1

- **JPPFSubmissionManager** (204 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.libmanagement
**Files:** 1

- **Downloader** (177 lines)
  - Patterns: MODEL_STOP_LOCATION

### org.jppf.logging.jdk
**Files:** 3

- **JmxHandler** (75 lines)
- **JPPFFileHandler** (52 lines)
- **JPPFLogFormatter** (100 lines)

### org.jppf.logging.jmx
**Files:** 2

- **send** (200 lines)
- **JmxLoggerImpl** (73 lines)

### org.jppf.logging.log4j
**Files:** 1

- **JmxAppender** (117 lines)

### org.jppf.management
**Files:** 5

- **of** (110 lines)
- **TaskExecutionNotification** (54 lines)
- **JMXConnectionWrapper** (414 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **JPPFNodeAdmin** (307 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **JPPFNodeTaskMonitor** (152 lines)

### org.jppf.net
**Files:** 3

- **AbstractIPAddressPattern** (207 lines)
- **IPv4AddressPattern** (102 lines)
- **IPv6AddressPattern** (115 lines)

### org.jppf.node
**Files:** 2

- **AbstractMonitoredNode** (196 lines)
  - Patterns: UEC_PROCESSING, MODEL_STOP_FREQUENCY
- **MonitoredNode** (64 lines)
  - Patterns: MODEL_STOP_FREQUENCY

### org.jppf.node.event
**Files:** 3

- **to** (33 lines)
- **NodeLifeCycleListener** (52 lines)
- **describe** (69 lines)

### org.jppf.node.idle
**Files:** 5

- **IdleDetectionTask** (167 lines)
- **detects** (147 lines)
- **IdleStateEvent** (61 lines)
- **IdleTimeDetector** (32 lines)
- **IdleTimeDetectorFactory** (32 lines)

### org.jppf.node.policy
**Files:** 16

- **build** (376 lines)
- **AtLeast** (92 lines)
- **AtMost** (92 lines)
- **BetweenEE** (103 lines)
- **BetweenEI** (103 lines)
  - ... and 11 more files

### org.jppf.node.screensaver
**Files:** 2

- **JPPFScreenSaver** (488 lines)
- **enables** (281 lines)
  - Patterns: MODEL_STOP_FREQUENCY

### org.jppf.process
**Files:** 2

- **isten** (61 lines)
- **read** (160 lines)

### org.jppf.scheduling
**Files:** 1

- **contain** (200 lines)

### org.jppf.scripting
**Files:** 4

- **GroovyScriptRunner** (98 lines)
- **JPPFScriptingException** (55 lines)
- **RhinoScriptRunner** (233 lines)
- **ScriptRunnerFactory** (45 lines)

### org.jppf.security
**Files:** 5

- **CryptoUtils** (298 lines)
  - Patterns: UEC_PROCESSING
- **provided** (41 lines)
- **JPPFPermissions** (155 lines)
- **JPPFPolicy** (142 lines)
- **JPPFSecurityException** (56 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.serialization
**Files:** 5

- **builds** (130 lines)
- **JPPFConfigurationObjectStreamBuilder** (117 lines)
- **JPPFObjectStreamBuilder** (43 lines)
- **JPPFObjectStreamBuilderImpl** (54 lines)
- **relies** (131 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.server
**Files:** 4

- **ChannelContext** (41 lines)
- **DriverInitializer** (129 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **serves** (515 lines)
  - Patterns: MODEL_INDIVIDUAL_TOUR
- **ShutdownRestartTask** (101 lines)

### org.jppf.server.app
**Files:** 4

- **is** (224 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **files** (188 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **listens** (55 lines)
- **listen** (191 lines)

### org.jppf.server.job
**Files:** 1

- **associate** (56 lines)

### org.jppf.server.job.management
**Files:** 6

- **are** (90 lines)
- **TestDriverJobManagementMBean** (113 lines)
- **DriverJobManagementMBean** (79 lines)
- **NodeJobInformation** (55 lines)
- **DriverJobManagement** (288 lines)
  - ... and 1 more files

### org.jppf.server.nio
**Files:** 12

- **loader** (190 lines)
- **perform** (93 lines)
- **rely** (456 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **NioState** (36 lines)
- **define** (90 lines)
  - Patterns: MODEL_STOP_FREQUENCY
  - ... and 7 more files

### org.jppf.server.nio.classloader
**Files:** 3

- **encapsulates** (49 lines)
- **SendingNodeInitialResponseState** (75 lines)
- **SendingProviderInitialResponseState** (75 lines)

### org.jppf.server.nio.multiplexer.generic
**Files:** 10

- **act** (63 lines)
- **IdentifyingInboundChannelState** (76 lines)
- **IdleState** (61 lines)
- **MultiplexerContext** (310 lines)
- **MultiplexerNioServer** (223 lines)
  - ... and 5 more files

### org.jppf.server.nio.nodeserver
**Files:** 13

- **used** (89 lines)
  - Patterns: UEC_PROCESSING
- **for** (58 lines)
- **represents** (82 lines)
- **serve** (472 lines)
  - Patterns: UEC_PROCESSING, MODEL_STOP_FREQUENCY
- **representing** (185 lines)
  - ... and 8 more files

### org.jppf.server.node
**Files:** 6

- **handles** (156 lines)
- **hold** (41 lines)
- **manage** (555 lines)
- **defines** (48 lines)
- **NodeTaskWrapper** (129 lines)
  - ... and 1 more files

### org.jppf.server.node.local
**Files:** 1

- **JPPFLocalNode** (110 lines)

### org.jppf.server.node.remote
**Files:** 3

- **represent** (109 lines)
- **performs** (152 lines)
  - Patterns: MODEL_STOP_LOCATION
- **encapsulate** (175 lines)

### org.jppf.server.node.spi
**Files:** 2

- **JPPFDefaultNodeMBeanProvider** (65 lines)
- **JPPFNodeTaskMonitorProvider** (67 lines)

### org.jppf.server.peer
**Files:** 1

- **discover** (155 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.server.protocol
**Files:** 10

- **TestJPPFJobSLA** (169 lines)
- **TestJPPFTask** (127 lines)
- **group** (492 lines)
- **wraps** (160 lines)
- **FileLocation** (79 lines)
  - ... and 5 more files

### org.jppf.server.queue
**Files:** 4

- **also** (92 lines)
- **JPPFPriorityQueue** (526 lines)
- **JPPFQueue** (68 lines)
- **QueueListener** (34 lines)

### org.jppf.server.scheduler.bundle
**Files:** 4

- **AbstractBundler** (132 lines)
- **acts** (174 lines)
- **if** (39 lines)
- **LoadBalancingInformation** (68 lines)

### org.jppf.server.scheduler.bundle.autotuned
**Files:** 1

- **AbstractAutoTuneProfile** (31 lines)

### org.jppf.server.scheduler.bundle.fixedsize
**Files:** 1

- **FixedSizeProfile** (80 lines)

### org.jppf.server.scheduler.bundle.impl
**Files:** 5

- **implements** (205 lines)
- **AutotunedDelegatingBundler** (123 lines)
- **NodeSimulator** (267 lines)
- **ProportionalBundler** (77 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **RLBundler** (77 lines)

### org.jppf.server.scheduler.bundle.proportional
**Files:** 2

- **AbstractProportionalBundler** (215 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **ProportionalTuneProfile** (154 lines)

### org.jppf.server.scheduler.bundle.providers
**Files:** 4

- **AutoTunedBundlerProvider** (64 lines)
- **FixedSizeBundlerProvider** (63 lines)
- **ProportionalBundlerProvider** (66 lines)
- **RLBundlerProvider** (65 lines)

### org.jppf.server.scheduler.bundle.rl
**Files:** 2

- **AbstractRLBundler** (202 lines)
- **RLProfile** (142 lines)

### org.jppf.server.scheduler.bundle.spi
**Files:** 1

- **shall** (61 lines)

### org.jppf.server.spi
**Files:** 1

- **JPPFDefaultDriverMBeanProvider** (62 lines)

### org.jppf.startup
**Files:** 2

- **class** (41 lines)
- **JPPFStartupLoader** (63 lines)

### org.jppf.task.storage
**Files:** 3

- **provide** (45 lines)
- **ClientDataProvider** (104 lines)
- **MemoryMapDataProvider** (55 lines)

### org.jppf.test.setup
**Files:** 8

- **starts** (111 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **DriverProcessLauncher** (41 lines)
- **LifeCycleTask** (160 lines)
- **NodeProcessLauncher** (40 lines)
- **Setup1D1N1C** (117 lines)
  - Patterns: MODEL_STOP_FREQUENCY
  - ... and 3 more files

### org.jppf.ui.actions
**Files:** 5

- **AbstractActionHandler** (78 lines)
- **ActionHandler** (50 lines)
- **ActionHolder** (32 lines)
- **ActionsInitializer** (100 lines)
- **UpdatableAction** (38 lines)
  - Patterns: UEC_PROCESSING

### org.jppf.ui.monitoring
**Files:** 1

- **MonitorTableModel** (82 lines)

### org.jppf.ui.monitoring.charts.config
**Files:** 1

- **manages** (74 lines)

### org.jppf.ui.monitoring.data
**Files:** 1

- **StatsConstants** (74 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### org.jppf.ui.monitoring.event
**Files:** 2

- **StatsHandlerEvent** (77 lines)
- **StatsHandlerListener** (33 lines)

### org.jppf.ui.monitoring.job
**Files:** 7

- **JobDataPanel** (369 lines)
- **JobDataPanelActionManager** (38 lines)
- **JobNotificationListener** (88 lines)
- **JobRenderer** (100 lines)
- **JobTableCellRenderer** (65 lines)
  - ... and 2 more files

### org.jppf.ui.monitoring.job.actions
**Files:** 6

- **AbstractSuspendJobAction** (100 lines)
- **CancelJobAction** (87 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **ResumeJobAction** (98 lines)
- **SuspendJobAction** (46 lines)
- **SuspendRequeueJobAction** (46 lines)
  - ... and 1 more files

### org.jppf.ui.monitoring.node
**Files:** 6

- **HTMLPropertiesTableFormat** (120 lines)
- **JPPFNodeTreeTableModel** (164 lines)
- **NodeDataPanel** (458 lines)
- **NodeRenderer** (121 lines)
- **NodeTreeTableMouseListener** (176 lines)
  - Patterns: MODEL_STOP_FREQUENCY
  - ... and 1 more files

### org.jppf.ui.monitoring.node.actions
**Files:** 11

- **CancelTaskAction** (80 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **NodeConfigurationAction** (202 lines)
- **NodeInformationAction** (102 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **NodeThreadsAction** (167 lines)
- **ResetTaskCounterAction** (81 lines)
  - ... and 6 more files

### org.jppf.ui.options
**Files:** 20

- **AbstractOption** (190 lines)
  - Patterns: UEC_PROCESSING
- **AbstractOptionElement** (233 lines)
- **AbstractOptionProperties** (349 lines)
  - Patterns: UEC_PROCESSING
- **BooleanOption** (118 lines)
  - Patterns: UEC_PROCESSING
- **ButtonOption** (116 lines)
  - Patterns: UEC_PROCESSING
  - ... and 15 more files

### org.jppf.ui.options.event
**Files:** 2

- **ScriptedValueChangeListener** (122 lines)
  - Patterns: UEC_PROCESSING
- **ValueChangeEvent** (47 lines)
  - Patterns: UEC_PROCESSING

### org.jppf.ui.options.xml
**Files:** 1

- **DebugMouseListener** (117 lines)

### org.jppf.ui.treetable
**Files:** 9

- **AbstractCellEditor** (164 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **AbstractJPPFTreeTableModel** (148 lines)
- **AbstractTreeCellRenderer** (166 lines)
- **AbstractTreeTableModel** (312 lines)
  - Patterns: MODEL_INDIVIDUAL_TOUR
- **AbstractTreeTableOption** (170 lines)
  - Patterns: UEC_PROCESSING
  - ... and 4 more files

### org.jppf.ui.utils
**Files:** 1

- **GuiUtils** (117 lines)
  - Patterns: UEC_PROCESSING

### org.jppf.utils
**Files:** 25

- **that** (343 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **TestJPPFConfiguration** (73 lines)
- **DateTimeUtils** (53 lines)
  - Patterns: UEC_PROCESSING
- **FileReplacer** (218 lines)
  - Patterns: UEC_PROCESSING, MODEL_MANDATORY_TOUR
- **JPPFCallable** (31 lines)
  - ... and 20 more files

### sample
**Files:** 1

- **BaseDemoTask** (50 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### sample.allinone
**Files:** 1

- **AllInOne** (64 lines)

### sample.cascading
**Files:** 2

- **Task1** (62 lines)
- **Task2** (39 lines)

### sample.clientdataprovider
**Files:** 1

- **DataProviderTestTask** (106 lines)

### sample.datasize
**Files:** 2

- **DataTask** (84 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **MyCallable** (51 lines)

### sample.dist.commandline
**Files:** 1

- **ListDirectoryTask** (90 lines)

### sample.dist.matrix
**Files:** 3

- **ExtMatrixTask** (100 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **MatrixTask** (88 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **MyCustomPolicy** (50 lines)

### sample.dist.notification
**Files:** 1

- **simulate** (86 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### sample.dist.taskcommunication
**Files:** 4

- **MyTask** (110 lines)
  - Patterns: MODEL_STOP_FREQUENCY
- **MyTask1** (54 lines)
- **MyTask2** (54 lines)
- **MyTaskRunner** (68 lines)

### sample.dist.xstream
**Files:** 1

- **XstreamTask** (95 lines)
  - Patterns: MODEL_POPULATION_SYNTHESIS

### sample.helloworld
**Files:** 10

- **HelloWorld** (39 lines)
- **HelloWorldAnnotated** (47 lines)
- **HelloWorldAnnotatedConstructor** (59 lines)
- **HelloWorldAnnotatedStatic** (47 lines)
- **HelloWorldCallable** (45 lines)
  - ... and 5 more files

### sample.misc
**Files:** 1

- **DriverAndNodeLauncher** (60 lines)

### sample.nbody
**Files:** 4

- **NBody** (60 lines)
- **NBodyPanel** (159 lines)
- **NBodyTask** (111 lines)
- **Vector2d** (153 lines)

### sample.prime
**Files:** 2

- **LargeInt** (219 lines)
- **PrimeTask** (121 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### sample.taskmonitor
**Files:** 1

- **MBeanClient** (108 lines)

### sample.test
**Files:** 18

- **fails** (47 lines)
- **ConstantTask** (44 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **DB2LoadingTask** (48 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **ExceptionTestTask** (57 lines)
- **ExecutionReport** (43 lines)
  - ... and 13 more files

### sample.test.clientpool
**Files:** 1

- **PoolConfigGenerator** (74 lines)

### sample.test.job.management
**Files:** 1

- **JobManagementTestRunner** (93 lines)

### sample.test.junit
**Files:** 3

- **tests** (175 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **AllTests** (43 lines)
  - Patterns: MODEL_MANDATORY_TOUR
- **TestJUnit** (60 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### sample.test.profiling
**Files:** 1

- **do** (54 lines)

### test.annotated
**Files:** 1

- **AnnotatedTask** (76 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### test.classversion
**Files:** 3

- **ExampleGridRunner** (69 lines)
- **ExampleTask** (41 lines)
- **TaskVersion** (70 lines)
  - Patterns: MODEL_MANDATORY_TOUR

### test.encryption
**Files:** 1

- **TestEncryption** (69 lines)

### test.generic
**Files:** 3

- **CallableTask** (51 lines)
- **GenericRunner** (107 lines)
- **LotsOfOutputTask** (61 lines)

### test.ipprobe
**Files:** 1

- **Probe** (110 lines)

### test.jmx
**Files:** 1

- **TestJMX** (89 lines)

### test.nathalie
**Files:** 6

- **Application** (16 lines)
- **Beginning** (37 lines)
- **Execution** (34 lines)
- **Final** (21 lines)
- **Intercal** (9 lines)
  - ... and 1 more files

### test.node.nativelib
**Files:** 2

- **NativeLibLoader** (39 lines)
- **NativeLibRunner** (186 lines)

### test.node.tasktimeout
**Files:** 1

- **TimeoutTaskRunner** (186 lines)

### test.priority
**Files:** 3

- **PrioritizedTask** (51 lines)
- **PriorityTestRunner** (118 lines)
- **WaitTask** (59 lines)

### test.socket
**Files:** 1

- **SocketPerformance** (254 lines)

## Core CT-RAMP Components

### Model Runners (2 files)

**CtrampApplication**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 2149 lines
- Component: main_runner
- Implements: Serializable

**CreateLogsums**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 405 lines
- Component: main_runner

### Uec Processors (170 files)

**Constants**
- Package: `com.pb.common.calculator2`
- Size: 63 lines
- Component: None

**ControlFileReader**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 626 lines
- Component: mandatory_tour
- Implements: Serializable

**DataEntry**
- Package: `com.pb.common.calculator2`
- Size: 51 lines
- Component: None
- Implements: Serializable

**Expression**
- Package: `com.pb.common.calculator2`
- Size: 979 lines
- Component: None
- Implements: Constants, Serializable

**ExpressionFlags**
- Package: `com.pb.common.calculator2`
- Size: 33 lines
- Component: None
- Implements: Serializable

**ExpressionIndex**
- Package: `com.pb.common.calculator2`
- Size: 66 lines
- Component: None
- Implements: Serializable

**IndexValues**
- Package: `com.pb.common.calculator2`
- Size: 35 lines
- Component: None
- Implements: Serializable

**LinkCalculator**
- Package: `com.pb.common.calculator`
- Size: 228 lines
- Component: None
- Implements: VariableTable, Serializable

**LinkFunction**
- Package: `com.pb.common.calculator`
- Size: 191 lines
- Component: None
- Implements: Serializable

**MatrixCalculator**
- Package: `com.pb.common.calculator`
- Size: 241 lines
- Component: None
- Implements: VariableTable, Serializable

**MatrixDataManager**
- Package: `com.pb.common.calculator2`
- Size: 618 lines
- Component: mandatory_tour
- Implements: Serializable

**MatrixDataServerIf**
- Package: `com.pb.common.calculator2`
- Size: 19 lines
- Component: stop_frequency

**implementation**
- Package: `com.pb.common.calculator`
- Size: 31 lines
- Component: None

**used**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 89 lines
- Component: None

**ModelAlternative**
- Package: `com.pb.common.calculator2`
- Size: 36 lines
- Component: None
- Implements: Serializable

**ModelEntry**
- Package: `com.pb.common.calculator2`
- Size: 41 lines
- Component: None
- Implements: Serializable

**ModelHeader**
- Package: `com.pb.common.calculator2`
- Size: 50 lines
- Component: None
- Implements: Serializable

**TableDataSetManager**
- Package: `com.pb.common.calculator2`
- Size: 173 lines
- Component: population_synthesis
- Implements: Serializable

**UtilityExpressionCalculator**
- Package: `com.pb.common.newmodel`
- Size: 2326 lines
- Component: population_synthesis
- Implements: VariableTable, Serializable

**VariableType**
- Package: `com.pb.common.calculator2`
- Size: 37 lines
- Component: None
- Implements: Serializable

**TableDataIndex**
- Package: `com.pb.common.calculator2`
- Size: 15 lines
- Component: None
- Implements: Serializable

**and**
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- Component: population_synthesis

**reads**
- Package: `com.pb.mtctm2.abm.survey`
- Size: 385 lines
- Component: accessibility

**NEW_CSVFileReader**
- Package: `com.pb.common.datafile`
- Size: 647 lines
- Component: None
- Extends: TableDataFileReader
- Implements: DataTypes

**that**
- Package: `org.jppf.utils`
- Size: 343 lines
- Component: mandatory_tour

**HttpWorker**
- Package: `com.pb.common.http`
- Size: 267 lines
- Component: mandatory_tour

**serve**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 472 lines
- Component: stop_frequency

**LogExpCalculator**
- Package: `com.pb.common.math`
- Size: 75 lines
- Component: None

**MatrixMerge**
- Package: `com.pb.common.matrix`
- Size: 178 lines
- Component: mandatory_tour

**Alternative**
- Package: `com.pb.common.newmodel`
- Size: 118 lines
- Component: None

**ChoiceModelApplication**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 649 lines
- Component: population_synthesis
- Implements: java.io.Serializable

**ConcreteAlternative**
- Package: `com.pb.common.newmodel`
- Size: 179 lines
- Component: None
- Implements: Alternative, Serializable

**DiscreteChoiceModel**
- Package: `com.pb.common.model`
- Size: 365 lines
- Component: None
- Implements: CompositeAlternative

**EventModel**
- Package: `com.pb.common.model`
- Size: 434 lines
- Component: mandatory_tour

**LogitModel**
- Package: `com.pb.common.newmodel`
- Size: 464 lines
- Component: None
- Implements: Alternative, Serializable

**LogitModelWithLogExpCalculator**
- Package: `com.pb.common.model`
- Size: 113 lines
- Component: trip_mode
- Extends: LogitModel

**ModelException**
- Package: `com.pb.common.model`
- Size: 47 lines
- Component: None
- Extends: RuntimeException

**just**
- Package: `com.pb.common.model`
- Size: 117 lines
- Component: None

**MultinomialLogitModel**
- Package: `com.pb.common.newmodel`
- Size: 154 lines
- Component: None

**MergeSummitFiles**
- Package: `com.pb.common.summit`
- Size: 235 lines
- Component: individual_tour

**SummitRecordTable**
- Package: `com.pb.common.summit`
- Size: 238 lines
- Component: stop_location

**ObjectUtil**
- Package: `com.pb.common.util`
- Size: 221 lines
- Component: mandatory_tour

**in**
- Package: `com.pb.common.util`
- Size: 135 lines
- Component: None

**will**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 740 lines
- Component: population_synthesis

**DMU**
- Package: `com.pb.common.calculator2.tests`
- Size: 77 lines
- Component: mandatory_tour
- Implements: VariableTable, Serializable

**ExpressionTest**
- Package: `com.pb.common.calculator2.tests`
- Size: 197 lines
- Component: None
- Implements: VariableTable

**LinkFunctionTest**
- Package: `com.pb.common.calculator2.tests`
- Size: 109 lines
- Component: mandatory_tour

**MatrixCalculatorTest**
- Package: `com.pb.common.calculator2.tests`
- Size: 115 lines
- Component: None

**UECTest**
- Package: `com.pb.common.calculator2.tests`
- Size: 111 lines
- Component: None
- Implements: Serializable

**TableDataSetStringIndexTest**
- Package: `com.pb.common.datafile.tests`
- Size: 601 lines
- Component: None

**LogExpCalculatorTest**
- Package: `com.pb.common.math.tests`
- Size: 116 lines
- Component: mandatory_tour
- Extends: TestCase

**MatrixTest**
- Package: `com.pb.common.matrix.tests`
- Size: 186 lines
- Component: None

**AccessibilitiesDMU**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 194 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**BestTransitPathCalculator**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 1588 lines
- Component: population_synthesis
- Implements: Serializable

**DcUtilitiesTaskJppf**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 516 lines
- Component: population_synthesis
- Extends: JPPFTask
- Implements: Callable<List<Object>>

**MandatoryAccessibilitiesDMU**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 206 lines
- Component: mandatory_tour
- Implements: Serializable, VariableTable

**StoredTransitSkimData**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 56 lines
- Component: None

**StoredUtilityData**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 102 lines
- Component: tour_mode_choice

**TransitPath**
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 45 lines
- Component: tour_mode_choice
- Implements: Comparable<TransitPath>

**MTCTM2TripTables**
- Package: `com.pb.mtctm2.abm.application`
- Size: 921 lines
- Component: population_synthesis

**SandagAtWorkSubtourFrequencyDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 64 lines
- Component: population_synthesis
- Extends: AtWorkSubtourFrequencyDMU
- Implements: VariableTable

**SandagDestChoiceSoaTwoStageTazDistUtilityDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 68 lines
- Component: population_synthesis
- Extends: DestChoiceTwoStageSoaTazDistanceUtilityDMU

**SandagModelStructure**
- Package: `com.pb.mtctm2.abm.application`
- Size: 1100 lines
- Component: mandatory_tour
- Extends: ModelStructure

**SandagStopFrequencyDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 250 lines
- Component: population_synthesis
- Extends: StopFrequencyDMU

**SandagSummitFile**
- Package: `com.pb.mtctm2.abm.application`
- Size: 756 lines
- Component: population_synthesis

**SandagTripModeChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 464 lines
- Component: population_synthesis
- Extends: TripModeChoiceDMU

**AtWorkSubtourFrequencyDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 203 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**AutoOwnershipChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 320 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**CoordinatedDailyActivityPatternDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 428 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**CtrampDmuFactoryIf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 48 lines
- Component: mandatory_tour

**DcSoaDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 216 lines
- Component: population_synthesis
- Implements: SoaDMU, Serializable, VariableTable

**DestChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 402 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**DestChoiceModelManager**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 918 lines
- Component: population_synthesis
- Implements: Serializable

**DestChoiceSize**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 914 lines
- Component: mandatory_tour
- Implements: Serializable

**DestChoiceTwoStageModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 569 lines
- Component: stop_frequency

**DestChoiceTwoStageModelDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 405 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**DestChoiceTwoStageSoaProbabilitiesCalculator**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 140 lines
- Component: None

**DestChoiceTwoStageSoaTazDistanceUtilityDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 176 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**DestinationSampleOfAlternativesModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 539 lines
- Component: population_synthesis
- Implements: Serializable

**Household**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1671 lines
- Component: population_synthesis
- Implements: java.io.Serializable

**HouseholdAtWorkSubtourFrequencyModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 333 lines
- Component: population_synthesis
- Implements: Serializable

**HouseholdAutoOwnershipModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 330 lines
- Component: population_synthesis
- Implements: Serializable

**HouseholdChoiceModelRunner**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 239 lines
- Component: population_synthesis

**HouseholdChoiceModels**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 828 lines
- Component: population_synthesis

**HouseholdChoiceModelsManager**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 503 lines
- Component: population_synthesis
- Implements: Serializable

**HouseholdChoiceModelsTaskJppf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 192 lines
- Component: population_synthesis
- Extends: JPPFTask

**HouseholdDataWriter**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1758 lines
- Component: population_synthesis

**HouseholdIndividualMandatoryTourDepartureAndDurationTime**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1550 lines
- Component: population_synthesis
- Implements: Serializable

**HouseholdIndividualMandatoryTourFrequencyModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 404 lines
- Component: population_synthesis
- Implements: Serializable

**HouseholdIndividualNonMandatoryTourFrequencyModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 773 lines
- Component: population_synthesis
- Implements: Serializable

**IndividualMandatoryTourFrequencyDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 291 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**IndividualNonMandatoryTourFrequencyDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 752 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**JointTourModels**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 811 lines
- Component: population_synthesis
- Implements: Serializable

**JointTourModelsDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 312 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**MandatoryDestChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 665 lines
- Component: population_synthesis
- Implements: Serializable

**MatrixDataServer**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 181 lines
- Component: stop_frequency
- Implements: MatrixDataServerIf, Serializable

**MatrixDataServerRmi**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 81 lines
- Component: stop_frequency
- Implements: MatrixDataServerIf, Serializable

**McLogsumsCalculator**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 756 lines
- Component: population_synthesis
- Implements: Serializable

**ModelStructure**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 606 lines
- Component: population_synthesis
- Implements: Serializable

**MyLogit**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 113 lines
- Component: None
- Extends: LogitModel

**NonMandatoryDestChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1281 lines
- Component: population_synthesis
- Implements: Serializable

**NonMandatoryTourDepartureAndDurationTime**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1213 lines
- Component: population_synthesis
- Implements: Serializable

**ParkingChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 295 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**ParkingProvisionChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 233 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**ParkingProvisionModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 201 lines
- Component: population_synthesis
- Implements: Serializable

**Person**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1951 lines
- Component: population_synthesis
- Implements: java.io.Serializable

**SchoolLocationChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 579 lines
- Component: population_synthesis
- Implements: Serializable

**SchoolLocationChoiceTaskJppf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 171 lines
- Component: population_synthesis
- Extends: JPPFTask

**SchoolLocationChoiceTaskJppfNew**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 184 lines
- Component: population_synthesis
- Extends: JPPFTask

**StopDCSoaDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 159 lines
- Component: tour_mode_choice
- Implements: Serializable, VariableTable

**SubtourDepartureAndDurationTime**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 849 lines
- Component: population_synthesis
- Implements: Serializable

**SubtourDestChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1130 lines
- Component: population_synthesis
- Implements: Serializable

**TimeDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 103 lines
- Component: mandatory_tour
- Implements: Serializable, VariableTable

**TNCAndTaxiWaitTimeCalculator**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 218 lines
- Component: population_synthesis

**Tour**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- Component: population_synthesis
- Implements: Serializable

**TourDepartureTimeAndDurationDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 846 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**TourModeChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 481 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**TourModeChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 529 lines
- Component: population_synthesis
- Implements: Serializable

**TourVehicleTypeChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 191 lines
- Component: population_synthesis
- Implements: Serializable

**TransitSubsidyAndPassDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 193 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**TransitSubsidyAndPassModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 544 lines
- Component: population_synthesis
- Implements: Serializable

**TransponderChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 157 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**TransponderChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 126 lines
- Component: population_synthesis
- Implements: Serializable

**TripModeChoiceDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 737 lines
- Component: population_synthesis
- Implements: Serializable, VariableTable

**UsualWorkSchoolLocationChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 778 lines
- Component: population_synthesis
- Implements: Serializable

**WorkLocationChoiceModel**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 662 lines
- Component: population_synthesis
- Implements: Serializable

**WorkLocationChoiceTaskJppf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 175 lines
- Component: population_synthesis
- Extends: JPPFTask

**WorkLocationChoiceTaskJppfNew**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 191 lines
- Component: population_synthesis
- Extends: JPPFTask

**DataExporter**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 1777 lines
- Component: population_synthesis

**SkimBuilder**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 559 lines
- Component: population_synthesis

**DeadheadModel**
- Package: `com.pb.mtctm2.abm.transitcapacityrestraint`
- Size: 345 lines
- Component: population_synthesis

**ParkingCapacityRestraintModel**
- Package: `com.pb.mtctm2.abm.transitcapacityrestraint`
- Size: 1075 lines
- Component: population_synthesis

**chooses**
- Package: `com.pb.mtctm2.abm.transitcapacityrestraint`
- Size: 768 lines
- Component: population_synthesis

**UpdatableAction**
- Package: `org.jppf.ui.actions`
- Size: 38 lines
- Component: None
- Extends: Action

**AbstractOption**
- Package: `org.jppf.ui.options`
- Size: 190 lines
- Component: None
- Extends: AbstractOptionElement
- Implements: Option

**AbstractOptionProperties**
- Package: `org.jppf.ui.options`
- Size: 349 lines
- Component: None
- Implements: OptionProperties

**BooleanOption**
- Package: `org.jppf.ui.options`
- Size: 118 lines
- Component: None
- Extends: AbstractOption

**ButtonOption**
- Package: `org.jppf.ui.options`
- Size: 116 lines
- Component: None
- Extends: AbstractOption

**ComboBoxOption**
- Package: `org.jppf.ui.options`
- Size: 195 lines
- Component: None
- Extends: AbstractOption

**FileChooserOption**
- Package: `org.jppf.ui.options`
- Size: 271 lines
- Component: None
- Extends: AbstractOption

**FillerOption**
- Package: `org.jppf.ui.options`
- Size: 90 lines
- Component: None
- Extends: AbstractOption

**JavaOption**
- Package: `org.jppf.ui.options`
- Size: 158 lines
- Component: None
- Extends: AbstractOption

**LabelOption**
- Package: `org.jppf.ui.options`
- Size: 93 lines
- Component: None
- Extends: AbstractOption

**ListOption**
- Package: `org.jppf.ui.options`
- Size: 245 lines
- Component: stop_frequency
- Extends: AbstractOption

**RadioButtonOption**
- Package: `org.jppf.ui.options`
- Size: 118 lines
- Component: None
- Extends: AbstractOption

**SpinnerNumberOption**
- Package: `org.jppf.ui.options`
- Size: 194 lines
- Component: None
- Extends: AbstractOption

**TextAreaOption**
- Package: `org.jppf.ui.options`
- Size: 198 lines
- Component: None
- Extends: AbstractOption

**TextOption**
- Package: `org.jppf.ui.options`
- Size: 122 lines
- Component: None
- Extends: AbstractOption

**ToolbarSeparatorOption**
- Package: `org.jppf.ui.options`
- Size: 101 lines
- Component: None
- Extends: AbstractOption

**AbstractTreeTableOption**
- Package: `org.jppf.ui.treetable`
- Size: 170 lines
- Component: None
- Extends: AbstractOption

**JTreeTable**
- Package: `org.jppf.ui.treetable`
- Size: 515 lines
- Component: mandatory_tour
- Extends: JTable

**GuiUtils**
- Package: `org.jppf.ui.utils`
- Size: 117 lines
- Component: None

**ScriptedValueChangeListener**
- Package: `org.jppf.ui.options.event`
- Size: 122 lines
- Component: None
- Implements: ValueChangeListener

**ValueChangeEvent**
- Package: `org.jppf.ui.options.event`
- Size: 47 lines
- Component: None
- Extends: EventObject

**IOHelper**
- Package: `org.jppf.io`
- Size: 298 lines
- Component: mandatory_tour

**DateTimeUtils**
- Package: `org.jppf.utils`
- Size: 53 lines
- Component: None

**FileReplacer**
- Package: `org.jppf.utils`
- Size: 218 lines
- Component: mandatory_tour

**LocalizationUtils**
- Package: `org.jppf.utils`
- Size: 111 lines
- Component: None

**ReflectionHelper**
- Package: `org.jppf.utils`
- Size: 218 lines
- Component: None

**ReflectionUtils**
- Package: `org.jppf.utils`
- Size: 369 lines
- Component: None

**SerializationHelperImpl**
- Package: `org.jppf.utils`
- Size: 83 lines
- Component: None
- Implements: SerializationHelper

**AbstractMonitoredNode**
- Package: `org.jppf.node`
- Size: 196 lines
- Component: stop_frequency
- Extends: ThreadSynchronization
- Implements: MonitoredNode, Runnable

**CryptoUtils**
- Package: `org.jppf.security`
- Size: 298 lines
- Component: None

**CompressionUtils**
- Package: `org.jppf.utils`
- Size: 104 lines
- Component: None

**holding**
- Package: `org.jppf.utils`
- Size: 110 lines
- Component: None

**SerializationHelper**
- Package: `org.jppf.utils`
- Size: 34 lines
- Component: None

**SerializationUtils**
- Package: `org.jppf.utils`
- Size: 181 lines
- Component: None

**SystemUtils**
- Package: `org.jppf.utils`
- Size: 341 lines
- Component: mandatory_tour

**RegExp**
- Package: `org.jppf.node.policy`
- Size: 100 lines
- Component: None
- Extends: ExecutionPolicy

**ensures**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 248 lines
- Component: None

### Tour Models (77 files)

**D211FileReader**
- Package: `com.pb.common.datafile`
- Size: 290 lines
- Component: mandatory_tour
- Implements: Serializable

**D231FileReader**
- Package: `com.pb.common.datafile`
- Size: 129 lines
- Component: mandatory_tour
- Implements: Serializable

**DBFFileWriter**
- Package: `com.pb.common.datafile`
- Size: 200 lines
- Component: individual_tour
- Extends: TableDataFileWriter

**TableDataSet**
- Package: `com.pb.common.datafile`
- Size: 1967 lines
- Component: mandatory_tour
- Implements: DataTypes, Serializable

**TableDataSetCrosstabber**
- Package: `com.pb.common.datafile`
- Size: 407 lines
- Component: mandatory_tour

**Execute**
- Package: `com.pb.common.env`
- Size: 658 lines
- Component: mandatory_tour

**HttpRequestProcessor**
- Package: `com.pb.common.http`
- Size: 92 lines
- Component: mandatory_tour

**HttpServer**
- Package: `com.pb.common.http`
- Size: 209 lines
- Component: mandatory_tour

**replacement**
- Package: `com.pb.common.math`
- Size: 729 lines
- Component: mandatory_tour

**DiskBasedMatrix**
- Package: `com.pb.common.matrix`
- Size: 167 lines
- Component: individual_tour

**TpplusMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 228 lines
- Component: individual_tour
- Extends: MatrixReader

**TpplusMatrixReader64**
- Package: `com.pb.common.matrix`
- Size: 256 lines
- Component: individual_tour
- Extends: MatrixReader

**TranscadMatrix**
- Package: `com.pb.common.matrix`
- Size: 148 lines
- Component: mandatory_tour
- Extends: DiskBasedMatrix

**DiscreteChoiceModelHelper**
- Package: `com.pb.common.model`
- Size: 324 lines
- Component: mandatory_tour

**JDBCConnection**
- Package: `com.pb.common.sql`
- Size: 189 lines
- Component: mandatory_tour

**SummitFileReader**
- Package: `com.pb.common.summit`
- Size: 328 lines
- Component: mandatory_tour

**SummitFileWriter**
- Package: `com.pb.common.summit`
- Size: 272 lines
- Component: tour_tod

**SummitHeader**
- Package: `com.pb.common.summit`
- Size: 196 lines
- Component: tour_tod
- Implements: Serializable

**ThreadPool**
- Package: `com.pb.common.util`
- Size: 80 lines
- Component: mandatory_tour

**ThreadPoolWorker**
- Package: `com.pb.common.util`
- Size: 117 lines
- Component: mandatory_tour

**ThreadRunner**
- Package: `com.pb.common.util`
- Size: 183 lines
- Component: mandatory_tour

**ThreadViewerTableModel**
- Package: `com.pb.common.util`
- Size: 197 lines
- Component: mandatory_tour
- Extends: AbstractTableModel

**tests**
- Package: `sample.test.junit`
- Size: 175 lines
- Component: mandatory_tour

**TestNumericalIntegrator**
- Package: `com.pb.common.math.tests`
- Size: 28 lines
- Component: mandatory_tour
- Extends: TestCase

**TestRmiMatrixReader**
- Package: `com.pb.common.matrix.tests`
- Size: 95 lines
- Component: mandatory_tour

**MatrixUtilTest**
- Package: `com.pb.common.matrix.util`
- Size: 630 lines
- Component: mandatory_tour

**JDBCConnectionTest**
- Package: `com.pb.common.sql.tests`
- Size: 113 lines
- Component: mandatory_tour

**MySQLTest**
- Package: `com.pb.common.sql.tests`
- Size: 144 lines
- Component: mandatory_tour

**BooleanLockTest**
- Package: `com.pb.common.util.tests`
- Size: 113 lines
- Component: mandatory_tour

**PerformanceTimerTest**
- Package: `com.pb.common.util.tests`
- Size: 104 lines
- Component: mandatory_tour

**ResourceUtilTest**
- Package: `com.pb.common.util.tests`
- Size: 46 lines
- Component: mandatory_tour
- Extends: TestCase

**NodeZoneMapping**
- Package: `com.pb.mtctm2.abm.application`
- Size: 120 lines
- Component: mandatory_tour

**SandagCreateTripGenerationFiles**
- Package: `com.pb.mtctm2.abm.application`
- Size: 1057 lines
- Component: mandatory_tour

**SandagParkingProvisionChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 85 lines
- Component: mandatory_tour
- Extends: ParkingProvisionChoiceDMU

**SandagStopLocationDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 114 lines
- Component: joint_tour
- Extends: StopLocationDMU

**StopDestChoiceSize**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 213 lines
- Component: mandatory_tour
- Implements: Serializable

**ReportBuilder**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 275 lines
- Component: mandatory_tour

**AbstractTreeTableModel**
- Package: `org.jppf.ui.treetable`
- Size: 312 lines
- Component: individual_tour
- Implements: TreeTableModel

**FileSystemModel**
- Package: `org.jppf.ui.treetable`
- Size: 278 lines
- Component: mandatory_tour
- Extends: AbstractTreeTableModel
- Implements: TreeTableModel

**StatsConstants**
- Package: `org.jppf.ui.monitoring.data`
- Size: 74 lines
- Component: mandatory_tour

**NodeInformationAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 102 lines
- Component: mandatory_tour
- Extends: AbstractTopologyAction

**JPPFExecutorService**
- Package: `org.jppf.client.concurrent`
- Size: 453 lines
- Component: individual_tour
- Implements: ExecutorService, FutureResultCollectorListener

**ChannelInputSource**
- Package: `org.jppf.io`
- Size: 136 lines
- Component: mandatory_tour
- Implements: InputSource

**generate**
- Package: `org.jppf.utils`
- Size: 269 lines
- Component: mandatory_tour

**AbstractProportionalBundler**
- Package: `org.jppf.server.scheduler.bundle.proportional`
- Size: 215 lines
- Component: mandatory_tour
- Extends: AbstractBundler

**BaseDemoTask**
- Package: `sample`
- Size: 50 lines
- Component: mandatory_tour
- Extends: JPPFTask

**DataTask**
- Package: `sample.datasize`
- Size: 84 lines
- Component: mandatory_tour
- Extends: JPPFTask

**PrimeTask**
- Package: `sample.prime`
- Size: 121 lines
- Component: mandatory_tour
- Extends: JPPFTask

**ConstantTask**
- Package: `sample.test`
- Size: 44 lines
- Component: mandatory_tour
- Extends: JPPFTestTask

**DB2LoadingTask**
- Package: `sample.test`
- Size: 48 lines
- Component: mandatory_tour
- Extends: JPPFTestTask

**JPPFTestTask**
- Package: `sample.test`
- Size: 136 lines
- Component: mandatory_tour
- Extends: JPPFTask

**SecurityTestTask**
- Package: `sample.test`
- Size: 121 lines
- Component: mandatory_tour
- Extends: JPPFTestTask

**ExtMatrixTask**
- Package: `sample.dist.matrix`
- Size: 100 lines
- Component: mandatory_tour
- Extends: BaseDemoTask

**MatrixTask**
- Package: `sample.dist.matrix`
- Size: 88 lines
- Component: mandatory_tour
- Extends: BaseDemoTask

**simulate**
- Package: `sample.dist.notification`
- Size: 86 lines
- Component: mandatory_tour

**AllTests**
- Package: `sample.test.junit`
- Size: 43 lines
- Component: mandatory_tour

**TestJUnit**
- Package: `sample.test.junit`
- Size: 60 lines
- Component: mandatory_tour
- Extends: TestCase
- Implements: Serializable

**AnnotatedTask**
- Package: `test.annotated`
- Size: 76 lines
- Component: mandatory_tour
- Extends: JPPFTask

**TaskVersion**
- Package: `test.classversion`
- Size: 70 lines
- Component: mandatory_tour
- Extends: JPPFTask

**initiates**
- Package: `org.jppf.jca.spi`
- Size: 242 lines
- Component: mandatory_tour

**JcaSocketInitializer**
- Package: `org.jppf.jca.work`
- Size: 92 lines
- Component: mandatory_tour
- Extends: AbstractSocketInitializer

**run**
- Package: `org.jppf.jca.work`
- Size: 104 lines
- Component: mandatory_tour

**JPPFJcaResultCollector**
- Package: `org.jppf.jca.work`
- Size: 86 lines
- Component: mandatory_tour
- Implements: TaskResultListener

**JPPFSubmissionManager**
- Package: `org.jppf.jca.work.submission`
- Size: 204 lines
- Component: mandatory_tour
- Extends: ThreadSynchronization
- Implements: Work

**JMXConnectionWrapper**
- Package: `org.jppf.management`
- Size: 414 lines
- Component: mandatory_tour
- Extends: ThreadSynchronization

**JPPFSecurityException**
- Package: `org.jppf.security`
- Size: 56 lines
- Component: mandatory_tour
- Extends: JPPFException

**relies**
- Package: `org.jppf.serialization`
- Size: 131 lines
- Component: mandatory_tour

**broadcast**
- Package: `org.jppf.comm.discovery`
- Size: 158 lines
- Component: mandatory_tour

**ExecutionPolicy**
- Package: `org.jppf.node.policy`
- Size: 363 lines
- Component: mandatory_tour
- Implements: Serializable

**PolicyRuleTest**
- Package: `org.jppf.node.policy`
- Size: 60 lines
- Component: mandatory_tour
- Extends: ExecutionPolicy

**DriverInitializer**
- Package: `org.jppf.server`
- Size: 129 lines
- Component: mandatory_tour

**serves**
- Package: `org.jppf.server`
- Size: 515 lines
- Component: individual_tour

**TaskExecutionListener**
- Package: `org.jppf.server.node`
- Size: 34 lines
- Component: individual_tour
- Extends: EventListener

**discover**
- Package: `org.jppf.server.peer`
- Size: 155 lines
- Component: mandatory_tour

**ProportionalBundler**
- Package: `org.jppf.server.scheduler.bundle.impl`
- Size: 77 lines
- Component: mandatory_tour
- Extends: AbstractProportionalBundler

**CallCenter**
- Package: `None`
- Size: 197 lines
- Component: mandatory_tour

**CallEv**
- Package: `None`
- Size: 193 lines
- Component: mandatory_tour

### Choice Models (90 files)

**TableDataSetIndex**
- Package: `com.pb.common.datafile`
- Size: 225 lines
- Component: stop_frequency
- Implements: TableDataSet.ChangeListener

**ExecuteStreamHandler**
- Package: `com.pb.common.env`
- Size: 61 lines
- Component: stop_frequency

**PumpStreamHandler**
- Package: `com.pb.common.env`
- Size: 112 lines
- Component: stop_frequency
- Implements: ExecuteStreamHandler

**is**
- Package: `org.jppf.server.app`
- Size: 224 lines
- Component: stop_frequency

**files**
- Package: `org.jppf.server.app`
- Size: 188 lines
- Component: stop_frequency

**CSVMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 382 lines
- Component: stop_location
- Extends: MatrixReader

**Emme2311MatrixReader**
- Package: `com.pb.common.matrix`
- Size: 180 lines
- Component: stop_location
- Extends: MatrixReader

**Emme2MatrixReader**
- Package: `com.pb.common.matrix`
- Size: 419 lines
- Component: stop_location
- Extends: MatrixReader

**MatrixBalancerRM**
- Package: `com.pb.common.matrix`
- Size: 471 lines
- Component: stop_frequency

**MatrixCompression**
- Package: `com.pb.common.matrix`
- Size: 415 lines
- Component: stop_location

**expands**
- Package: `com.pb.common.matrix`
- Size: 322 lines
- Component: stop_location

**MatrixIO32BitJvm**
- Package: `com.pb.common.matrix`
- Size: 269 lines
- Component: stop_frequency
- Implements: Serializable

**MatrixUtil**
- Package: `com.pb.common.matrix.util`
- Size: 537 lines
- Component: stop_frequency

**RemoteMatrixDataServer**
- Package: `com.pb.common.matrix`
- Size: 95 lines
- Component: stop_frequency

**initializes**
- Package: `com.pb.common.util`
- Size: 314 lines
- Component: trip_mode

**PerformanceTimer**
- Package: `com.pb.common.util`
- Size: 209 lines
- Component: stop_frequency

**ThreadViewer**
- Package: `com.pb.common.util`
- Size: 102 lines
- Component: stop_frequency
- Extends: JPanel

**BinaryFileTest**
- Package: `com.pb.common.datafile.tests`
- Size: 105 lines
- Component: stop_frequency

**CSVFileReaderTest**
- Package: `com.pb.common.datafile.tests`
- Size: 83 lines
- Component: stop_frequency

**TableDataSetTest**
- Package: `com.pb.common.datafile.tests`
- Size: 322 lines
- Component: stop_frequency

**Benchmark**
- Package: `com.pb.common.math.tests`
- Size: 234 lines
- Component: stop_frequency

**MersenneTwisterTest**
- Package: `com.pb.common.math.tests`
- Size: 317 lines
- Component: population_synthesis

**CreateZipFile**
- Package: `com.pb.common.matrix.tests`
- Size: 122 lines
- Component: stop_frequency

**TestRmiMatrixIO**
- Package: `com.pb.common.matrix.tests`
- Size: 130 lines
- Component: stop_frequency

**holds**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 18 lines
- Component: population_synthesis

**MTCTM2TourBasedModel**
- Package: `com.pb.mtctm2.abm.application`
- Size: 314 lines
- Component: population_synthesis

**SandagAutoOwnershipChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 120 lines
- Component: population_synthesis
- Extends: AutoOwnershipChoiceDMU

**SandagCoordinatedDailyActivityPatternDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 168 lines
- Component: accessibility
- Extends: CoordinatedDailyActivityPatternDMU

**SandagDcSoaDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 96 lines
- Component: population_synthesis
- Extends: DcSoaDMU

**SandagDestChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 176 lines
- Component: population_synthesis
- Extends: DestChoiceDMU

**SandagDestChoiceSoaTwoStageModelDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 166 lines
- Component: population_synthesis
- Extends: DestChoiceTwoStageModelDMU

**SandagHouseholdDataManager**
- Package: `com.pb.mtctm2.abm.application`
- Size: 597 lines
- Component: population_synthesis
- Extends: HouseholdDataManager

**SandagHouseholdDataManager2**
- Package: `com.pb.mtctm2.abm.application`
- Size: 712 lines
- Component: population_synthesis
- Extends: HouseholdDataManager

**SandagJointTourModelsDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 137 lines
- Component: population_synthesis
- Extends: JointTourModelsDMU

**SandagParkingChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 101 lines
- Component: population_synthesis
- Extends: ParkingChoiceDMU

**SandagSamplePopulationGenerator**
- Package: `com.pb.mtctm2.abm.application`
- Size: 100 lines
- Component: population_synthesis

**SandagTestSOA**
- Package: `com.pb.mtctm2.abm.application`
- Size: 197 lines
- Component: stop_frequency

**SandagTourModeChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 383 lines
- Component: population_synthesis
- Extends: TourModeChoiceDMU

**SandagTransitSubsidyAndPassDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 76 lines
- Component: population_synthesis
- Extends: TransitSubsidyAndPassDMU

**SandagTransponderChoiceDMU**
- Package: `com.pb.mtctm2.abm.application`
- Size: 60 lines
- Component: accessibility
- Extends: TransponderChoiceDMU

**HouseholdDataManager**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1908 lines
- Component: population_synthesis
- Implements: HouseholdDataManagerIf, Serializable

**HouseholdDataManagerIf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 136 lines
- Component: population_synthesis

**HouseholdDataManagerRmi**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 371 lines
- Component: population_synthesis
- Implements: HouseholdDataManagerIf, Serializable

**SoaDMU**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 10 lines
- Component: population_synthesis

**Stop**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 199 lines
- Component: population_synthesis
- Implements: Serializable

**TapDataManager**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 369 lines
- Component: parking
- Implements: Serializable

**TazDataIf**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 143 lines
- Component: parking

**ModelOutputReader**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 764 lines
- Component: population_synthesis

**SqlImportFileBuilder**
- Package: `com.pb.mtctm2.abm.reports`
- Size: 229 lines
- Component: population_synthesis

**AbstractCellEditor**
- Package: `org.jppf.ui.treetable`
- Size: 164 lines
- Component: stop_frequency
- Implements: CellEditor

**NodeTreeTableMouseListener**
- Package: `org.jppf.ui.monitoring.node`
- Size: 176 lines
- Component: stop_frequency
- Extends: MouseAdapter

**CancelJobAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 87 lines
- Component: stop_frequency
- Extends: AbstractJobAction

**CancelTaskAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 80 lines
- Component: stop_frequency
- Extends: JPPFAbstractNodeAction

**ServerShutdownRestartAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 186 lines
- Component: stop_frequency
- Extends: AbstractTopologyAction

**ShutdownNodeAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 87 lines
- Component: stop_frequency
- Extends: AbstractTopologyAction

**performs**
- Package: `org.jppf.server.node.remote`
- Size: 152 lines
- Component: stop_location

**TestJPPFClient**
- Package: `org.jppf.client`
- Size: 71 lines
- Component: stop_frequency
- Extends: Setup1D1N

**TestJPPFExecutorService**
- Package: `org.jppf.client.concurrent`
- Size: 255 lines
- Component: stop_frequency
- Extends: Setup1D1N1C

**starts**
- Package: `org.jppf.test.setup`
- Size: 111 lines
- Component: stop_frequency

**Setup1D1N1C**
- Package: `org.jppf.test.setup`
- Size: 117 lines
- Component: stop_frequency

**SamplesPHPReadmeProcessor**
- Package: `org.jppf.doc`
- Size: 150 lines
- Component: stop_location
- Implements: Runnable

**ChannelOutputDestination**
- Package: `org.jppf.io`
- Size: 92 lines
- Component: stop_location
- Implements: OutputDestination

**FileDataLocation**
- Package: `org.jppf.io`
- Size: 369 lines
- Component: stop_location
- Extends: AbstractDataLocation

**FileOutputDestination**
- Package: `org.jppf.io`
- Size: 60 lines
- Component: stop_location
- Extends: ChannelOutputDestination

**MultipleBuffersLocation**
- Package: `org.jppf.io`
- Size: 284 lines
- Component: stop_location
- Extends: AbstractDataLocation

**SocketWrapperOutputDestination**
- Package: `org.jppf.io`
- Size: 99 lines
- Component: stop_location
- Implements: OutputDestination

**StreamOutputDestination**
- Package: `org.jppf.io`
- Size: 105 lines
- Component: stop_location
- Implements: OutputDestination

**Downloader**
- Package: `org.jppf.libmanagement`
- Size: 177 lines
- Component: stop_location

**rely**
- Package: `org.jppf.server.nio`
- Size: 456 lines
- Component: stop_frequency

**define**
- Package: `org.jppf.server.nio`
- Size: 90 lines
- Component: stop_frequency

**SelectionKeyWrapper**
- Package: `org.jppf.server.nio`
- Size: 119 lines
- Component: stop_frequency
- Extends: AbstractChannelWrapper

**SecureKeyCipherTransform**
- Package: `org.jppf.example.dataencryption`
- Size: 176 lines
- Component: stop_location
- Implements: JPPFDataTransform

**DESCipherTransform**
- Package: `org.jppf.example.dataencryption.old`
- Size: 113 lines
- Component: stop_location
- Implements: JPPFDataTransform

**MyTask**
- Package: `sample.dist.taskcommunication`
- Size: 110 lines
- Component: stop_frequency
- Extends: JPPFTask
- Implements: ItemListener

**XstreamTask**
- Package: `sample.dist.xstream`
- Size: 95 lines
- Component: population_synthesis
- Extends: JPPFTask

**MonitoredNode**
- Package: `org.jppf.node`
- Size: 64 lines
- Component: stop_frequency
- Extends: Runnable

**MultipleBuffersInputStream**
- Package: `org.jppf.utils`
- Size: 198 lines
- Component: stop_location
- Extends: InputStream

**ClientConnection**
- Package: `org.jppf.comm.recovery`
- Size: 173 lines
- Component: stop_frequency
- Extends: AbstractRecoveryConnection

**BootstrapObjectSerializer**
- Package: `org.jppf.comm.socket`
- Size: 136 lines
- Component: stop_frequency
- Implements: ObjectSerializer

**attempt**
- Package: `org.jppf.comm.socket`
- Size: 199 lines
- Component: stop_frequency

**enables**
- Package: `org.jppf.node.screensaver`
- Size: 281 lines
- Component: stop_frequency

**JPPFNodeAdmin**
- Package: `org.jppf.management`
- Size: 307 lines
- Component: stop_frequency
- Implements: JPPFNodeAdminMBean, JPPFTaskListener, NodeListener

**RemoteNodeMessage**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 129 lines
- Component: stop_location
- Extends: AbstractNodeMessage

**Cafeteria**
- Package: `None`
- Size: 160 lines
- Component: stop_frequency

**Jobshop**
- Package: `None`
- Size: 129 lines
- Component: stop_frequency

**PreyPred**
- Package: `None`
- Size: 54 lines
- Component: stop_frequency

**QueueEv**
- Package: `None`
- Size: 73 lines
- Component: stop_frequency

**QueueProc**
- Package: `None`
- Size: 43 lines
- Component: stop_frequency

**TimeShared**
- Package: `None`
- Size: 87 lines
- Component: stop_frequency

**Visits**
- Package: `None`
- Size: 95 lines
- Component: stop_frequency

### Data Managers (22 files)

**BaseDataFile**
- Package: `com.pb.common.datafile`
- Size: 446 lines
- Component: None

**from**
- Package: `com.pb.common.datafile`
- Size: 715 lines
- Component: None

**DataFile**
- Package: `com.pb.common.datafile`
- Size: 232 lines
- Component: None
- Extends: BaseDataFile

**DataReader**
- Package: `com.pb.common.datafile`
- Size: 94 lines
- Component: None

**DataWriter**
- Package: `com.pb.common.datafile`
- Size: 105 lines
- Component: None

**DiskObjectArray**
- Package: `com.pb.common.datafile`
- Size: 227 lines
- Component: None
- Implements: Serializable

**FixedFormatTextFileReader**
- Package: `com.pb.common.datafile`
- Size: 315 lines
- Component: None
- Extends: TableDataFileReader

**writes**
- Package: `com.pb.common.datafile`
- Size: 136 lines
- Component: None

**TableDataSetCacheCollection**
- Package: `com.pb.common.datafile`
- Size: 218 lines
- Component: None
- Extends: TableDataSetCollection
- Implements: TableDataSet.TableDataSetWatcher

**TableDataSetCollection**
- Package: `com.pb.common.datafile`
- Size: 198 lines
- Component: None

**TableDataSetIndexedValue**
- Package: `com.pb.common.datafile`
- Size: 584 lines
- Component: None
- Implements: TableDataSetIndex.ChangeListener, java.io.Serializable, Cloneable

**TableDataSetSoftReference**
- Package: `com.pb.common.datafile`
- Size: 40 lines
- Component: None
- Extends: SoftReference

**DataStoreManager**
- Package: `com.pb.common.datastore`
- Size: 55 lines
- Component: None

**GridOperations**
- Package: `com.pb.common.grid`
- Size: 104 lines
- Component: None

**uses**
- Package: `com.pb.common.matrix`
- Size: 255 lines
- Component: None

**RmiMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 118 lines
- Component: None
- Extends: MatrixReader
- Implements: Serializable

**RmiMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 72 lines
- Component: None
- Extends: MatrixWriter
- Implements: Serializable

**DosCommand**
- Package: `com.pb.common.util`
- Size: 288 lines
- Component: None
- Implements: Serializable

**CSVFileWriterTest**
- Package: `com.pb.common.datafile.tests`
- Size: 64 lines
- Component: None

**DataFileTest**
- Package: `com.pb.common.datafile.tests`
- Size: 85 lines
- Component: None

**JDBCTableReaderTest**
- Package: `com.pb.common.datafile.tests`
- Size: 87 lines
- Component: None

**TestCSVMatrixReader**
- Package: `com.pb.common.matrix.tests`
- Size: 357 lines
- Component: None

### Utilities (446 files)

**for**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 58 lines
- Component: None

**to**
- Package: `org.jppf.node.event`
- Size: 33 lines
- Component: None

**DataHeader**
- Package: `com.pb.common.datafile`
- Size: 114 lines
- Component: None

**DataTypes**
- Package: `com.pb.common.datafile`
- Size: 38 lines
- Component: None
- Extends: Serializable

**DbByteArrayOutputStream**
- Package: `com.pb.common.datafile`
- Size: 47 lines
- Component: None
- Extends: ByteArrayOutputStream

**FileType**
- Package: `com.pb.common.datafile`
- Size: 55 lines
- Component: None
- Implements: Serializable

**provides**
- Package: `org.jppf.comm.socket`
- Size: 110 lines
- Component: None

**MissingValueException**
- Package: `com.pb.common.datafile`
- Size: 37 lines
- Component: None
- Extends: RuntimeException

**MultipleValueException**
- Package: `com.pb.common.datafile`
- Size: 39 lines
- Component: None
- Extends: RuntimeException

**ReportGenerator**
- Package: `com.pb.common.datafile`
- Size: 28 lines
- Component: None

**represents**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 82 lines
- Component: None

**JDBCManager**
- Package: `com.pb.common.datastore`
- Size: 43 lines
- Component: None

**Os**
- Package: `com.pb.common.env`
- Size: 199 lines
- Component: None

**ProcessDestroyer**
- Package: `com.pb.common.env`
- Size: 89 lines
- Component: None
- Extends: Thread

**ProcessEnvironment**
- Package: `com.pb.common.env`
- Size: 71 lines
- Component: None

**StreamPumper**
- Package: `com.pb.common.env`
- Size: 98 lines
- Component: None
- Implements: Runnable

**DataPoint**
- Package: `com.pb.common.graph`
- Size: 52 lines
- Component: None

**GraphData**
- Package: `com.pb.common.graph`
- Size: 31 lines
- Component: None

**MapValue**
- Package: `com.pb.common.graph`
- Size: 43 lines
- Component: None

**ThematicGraphData**
- Package: `com.pb.common.graph`
- Size: 38 lines
- Component: None
- Extends: GraphData

**XYGraphData**
- Package: `com.pb.common.graph`
- Size: 55 lines
- Component: None
- Extends: GraphData

**ConvertASCIItoGridFile**
- Package: `com.pb.common.grid`
- Size: 163 lines
- Component: None

**ConvertGridFiletoASCII**
- Package: `com.pb.common.grid`
- Size: 74 lines
- Component: None

**GridDataBuffer**
- Package: `com.pb.common.grid`
- Size: 165 lines
- Component: None

**GridFile**
- Package: `com.pb.common.grid`
- Size: 407 lines
- Component: None

**GridParameters**
- Package: `com.pb.common.grid`
- Size: 163 lines
- Component: None

**ClassFileServerTest**
- Package: `com.pb.common.http`
- Size: 55 lines
- Component: None

**ClassRunner**
- Package: `com.pb.common.http`
- Size: 89 lines
- Component: None

**Dependency**
- Package: `com.pb.common.http`
- Size: 8 lines
- Component: None

**loads**
- Package: `com.pb.common.http`
- Size: 79 lines
- Component: None

**TestContent**
- Package: `com.pb.common.http`
- Size: 22 lines
- Component: None
- Implements: Callable<String>

**ImageFactory**
- Package: `com.pb.common.image`
- Size: 335 lines
- Component: None

**listens**
- Package: `org.jppf.server.app`
- Size: 55 lines
- Component: None

**LogServerTest**
- Package: `com.pb.common.logging`
- Size: 32 lines
- Component: None

**MathNative**
- Package: `com.pb.common.math`
- Size: 40 lines
- Component: None

**contains**
- Package: `com.pb.common.util`
- Size: 39 lines
- Component: None

**NumericalIntegrator**
- Package: `com.pb.common.math`
- Size: 19 lines
- Component: None

**AlphaToBetaInterface**
- Package: `com.pb.common.matrix`
- Size: 69 lines
- Component: None

**BinaryMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 165 lines
- Component: None
- Extends: MatrixReader

**BinaryMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 146 lines
- Component: None
- Extends: MatrixWriter

**ColumnVector**
- Package: `com.pb.common.matrix`
- Size: 211 lines
- Component: None
- Extends: Matrix

**CrowbarMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 75 lines
- Component: None
- Extends: MatrixReader

**CrowbarMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 89 lines
- Component: None
- Extends: MatrixWriter

**CSVMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 178 lines
- Component: None
- Extends: MatrixWriter

**Emme2311MatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 65 lines
- Component: None
- Extends: MatrixWriter

**Emme2MatrixHelper**
- Package: `com.pb.common.matrix`
- Size: 90 lines
- Component: None

**Emme2MatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 204 lines
- Component: None
- Extends: MatrixWriter

**Epsilon**
- Package: `com.pb.common.matrix`
- Size: 53 lines
- Component: None

**ExternalNumberIterator**
- Package: `com.pb.common.matrix`
- Size: 62 lines
- Component: None
- Implements: Iterator

**IdentityMatrix**
- Package: `com.pb.common.matrix`
- Size: 43 lines
- Component: None
- Extends: SquareMatrix

**InvertibleMatrix**
- Package: `com.pb.common.matrix`
- Size: 104 lines
- Component: None
- Extends: LinearSystem

**LinearSystem**
- Package: `com.pb.common.matrix`
- Size: 383 lines
- Component: None
- Extends: SquareMatrix

**Matrix**
- Package: `com.pb.common.matrix`
- Size: 1663 lines
- Component: None
- Implements: java.io.Serializable

**MatrixBalancer**
- Package: `com.pb.common.matrix`
- Size: 430 lines
- Component: None

**MatrixConverter**
- Package: `com.pb.common.matrix`
- Size: 23 lines
- Component: None

**MatrixException**
- Package: `com.pb.common.matrix`
- Size: 60 lines
- Component: None
- Extends: RuntimeException

**representing**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 185 lines
- Component: None

**MatrixType**
- Package: `com.pb.common.matrix`
- Size: 104 lines
- Component: None
- Implements: Serializable

**NDimensionalMatrix**
- Package: `com.pb.common.matrix`
- Size: 1662 lines
- Component: None
- Implements: Cloneable, Serializable

**NDimensionalMatrixDouble**
- Package: `com.pb.common.matrix`
- Size: 1621 lines
- Component: None
- Implements: Cloneable, Serializable

**OMXMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 119 lines
- Component: None
- Extends: MatrixReader

**OMXMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 110 lines
- Component: None
- Extends: MatrixWriter

**OMXTest**
- Package: `com.pb.common.matrix`
- Size: 41 lines
- Component: None

**RemoteMatrixDataReader**
- Package: `com.pb.common.matrix`
- Size: 41 lines
- Component: None

**RowVector**
- Package: `com.pb.common.matrix`
- Size: 230 lines
- Component: None
- Extends: Matrix

**SquareMatrix**
- Package: `com.pb.common.matrix`
- Size: 108 lines
- Component: None
- Extends: Matrix

**TpplusMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 217 lines
- Component: None
- Extends: MatrixWriter

**TpplusMatrixWriter64**
- Package: `com.pb.common.matrix`
- Size: 240 lines
- Component: None
- Extends: MatrixWriter

**must**
- Package: `com.pb.common.matrix`
- Size: 133 lines
- Component: None

**allows**
- Package: `com.pb.common.matrix`
- Size: 63 lines
- Component: None

**TranscadMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 191 lines
- Component: None
- Extends: MatrixReader

**TranscadMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 196 lines
- Component: None
- Extends: MatrixWriter

**VisumMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 250 lines
- Component: None
- Extends: MatrixReader

**ZipMatrixReader**
- Package: `com.pb.common.matrix`
- Size: 224 lines
- Component: None
- Extends: MatrixReader

**ZipMatrixWriter**
- Package: `com.pb.common.matrix`
- Size: 224 lines
- Component: None
- Extends: MatrixWriter

**ChoiceStrategy**
- Package: `com.pb.common.model`
- Size: 32 lines
- Component: None

**CompositeAlternative**
- Package: `com.pb.common.model`
- Size: 31 lines
- Component: None
- Extends: Alternative

**DecisionMakerAttributes**
- Package: `com.pb.common.model`
- Size: 28 lines
- Component: None

**MonteCarloChoice**
- Package: `com.pb.common.model`
- Size: 61 lines
- Component: None
- Implements: ChoiceStrategy

**SQLHelper**
- Package: `com.pb.common.sql`
- Size: 63 lines
- Component: None

**also**
- Package: `org.jppf.server.queue`
- Size: 92 lines
- Component: None

**BinarySearch**
- Package: `com.pb.common.util`
- Size: 162 lines
- Component: None
- Implements: Serializable

**BooleanLock**
- Package: `com.pb.common.util`
- Size: 143 lines
- Component: None
- Extends: Object

**catch**
- Package: `com.pb.common.util`
- Size: 146 lines
- Component: None

**are**
- Package: `org.jppf.server.job.management`
- Size: 90 lines
- Component: None

**IndexSort**
- Package: `com.pb.common.util`
- Size: 157 lines
- Component: None

**ObjectPool**
- Package: `com.pb.common.util`
- Size: 201 lines
- Component: None

**of**
- Package: `org.jppf.management`
- Size: 110 lines
- Component: None

**PropertyMap**
- Package: `com.pb.common.util`
- Size: 317 lines
- Component: None
- Implements: Serializable

**StreamGobbler**
- Package: `com.pb.common.util`
- Size: 66 lines
- Component: None
- Extends: Thread

**StreamGobblerToLogfile**
- Package: `com.pb.common.util`
- Size: 53 lines
- Component: None
- Extends: Thread

**TimeWrapper**
- Package: `com.pb.common.util`
- Size: 57 lines
- Component: None

**DiskObjectArrayTest**
- Package: `com.pb.common.datafile.tests`
- Size: 193 lines
- Component: None

**Emme2DataBank**
- Package: `com.pb.common.emme2.io`
- Size: 880 lines
- Component: None

**Emme2FileParameters**
- Package: `com.pb.common.emme2.io`
- Size: 38 lines
- Component: None

**GlobalDatabankParameters**
- Package: `com.pb.common.emme2.io`
- Size: 41 lines
- Component: None

**TestProcessEnvironment**
- Package: `com.pb.common.env.tests`
- Size: 49 lines
- Component: None

**CreateMemoryMap**
- Package: `com.pb.common.grid.tests`
- Size: 68 lines
- Component: None

**GridFileTest**
- Package: `com.pb.common.grid.tests`
- Size: 397 lines
- Component: None

**SVGImageTest**
- Package: `com.pb.common.image.tests`
- Size: 86 lines
- Component: None

**JMath**
- Package: `com.pb.common.math.tests`
- Size: 2535 lines
- Component: None

**MatrixBalancerMain**
- Package: `com.pb.common.matrix.tests`
- Size: 269 lines
- Component: None

**MatrixBalancerTest**
- Package: `com.pb.common.matrix.tests`
- Size: 267 lines
- Component: None

**NDimensionalMatrixDoubleTest**
- Package: `com.pb.common.matrix.tests`
- Size: 87 lines
- Component: None

**TestBinaryMatrix**
- Package: `com.pb.common.matrix.tests`
- Size: 173 lines
- Component: None

**TestEmme2Matrix**
- Package: `com.pb.common.matrix.tests`
- Size: 136 lines
- Component: None

**TestMatrixWriter**
- Package: `com.pb.common.matrix.tests`
- Size: 115 lines
- Component: None

**TestTppMatrix**
- Package: `com.pb.common.matrix.tests`
- Size: 100 lines
- Component: None

**TestZipMatrix**
- Package: `com.pb.common.matrix.tests`
- Size: 170 lines
- Component: None

**UnzipFile**
- Package: `com.pb.common.matrix.tests`
- Size: 72 lines
- Component: None

**MatrixDataModel**
- Package: `com.pb.common.matrix.ui`
- Size: 62 lines
- Component: None
- Extends: AbstractTableModel

**MatrixNameDialog**
- Package: `com.pb.common.matrix.ui`
- Size: 127 lines
- Component: None
- Extends: JDialog

**MatrixViewer**
- Package: `com.pb.common.matrix.ui`
- Size: 180 lines
- Component: None
- Extends: JFrame

**MatrixViewerPanel**
- Package: `com.pb.common.matrix.ui`
- Size: 276 lines
- Component: None
- Extends: JPanel

**ColoredCellRenderer**
- Package: `com.pb.common.ui.swing`
- Size: 45 lines
- Component: None
- Extends: DefaultTableCellRenderer

**DecimalFormatRenderer**
- Package: `com.pb.common.ui.swing`
- Size: 43 lines
- Component: None
- Extends: DefaultTableCellRenderer

**HeaderRenderer**
- Package: `com.pb.common.ui.swing`
- Size: 72 lines
- Component: None
- Extends: DefaultTableCellRenderer

**JScrollPaneAdjuster**
- Package: `com.pb.common.ui.swing`
- Size: 179 lines
- Component: None
- Implements: PropertyChangeListener, Serializable

**JTableRowHeaderResizer**
- Package: `com.pb.common.ui.swing`
- Size: 193 lines
- Component: None
- Extends: MouseInputAdapter
- Implements: Serializable, ContainerListener

**MessageWindow**
- Package: `com.pb.common.ui.swing`
- Size: 76 lines
- Component: None
- Extends: JFrame
- Implements: java.io.Serializable

**RightAlignHeaderRenderer**
- Package: `com.pb.common.ui.swing`
- Size: 71 lines
- Component: None
- Extends: DefaultTableCellRenderer

**RowHeaderList**
- Package: `com.pb.common.ui.swing`
- Size: 77 lines
- Component: None

**RowHeaderRenderer**
- Package: `com.pb.common.ui.swing`
- Size: 126 lines
- Component: None
- Extends: DefaultTableCellRenderer
- Implements: ListCellRenderer

**RowHeaderResizer**
- Package: `com.pb.common.ui.swing`
- Size: 191 lines
- Component: None
- Extends: MouseInputAdapter
- Implements: Serializable, ContainerListener

**IndexMergeSortTest**
- Package: `com.pb.common.util.tests`
- Size: 56 lines
- Component: None

**builds**
- Package: `org.jppf.serialization`
- Size: 130 lines
- Component: None

**SandagCtrampApplication**
- Package: `com.pb.mtctm2.abm.application`
- Size: 23 lines
- Component: None
- Extends: CtrampApplication

**ConnectionHelper**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 55 lines
- Component: None

**DAOException**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 23 lines
- Component: None
- Extends: RuntimeException

**SqliteService**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 120 lines
- Component: None

**Util**
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 115 lines
- Component: None
- Implements: Serializable

**AbstractActionHandler**
- Package: `org.jppf.ui.actions`
- Size: 78 lines
- Component: None
- Implements: ActionHandler

**ActionHandler**
- Package: `org.jppf.ui.actions`
- Size: 50 lines
- Component: None

**ActionHolder**
- Package: `org.jppf.ui.actions`
- Size: 32 lines
- Component: None

**ActionsInitializer**
- Package: `org.jppf.ui.actions`
- Size: 100 lines
- Component: None
- Implements: Runnable

**MonitorTableModel**
- Package: `org.jppf.ui.monitoring`
- Size: 82 lines
- Component: None
- Extends: AbstractTableModel

**AbstractOptionElement**
- Package: `org.jppf.ui.options`
- Size: 233 lines
- Component: None
- Extends: AbstractOptionProperties
- Implements: OptionElement

**represent**
- Package: `org.jppf.server.node.remote`
- Size: 109 lines
- Component: None

**OptionPanel**
- Package: `org.jppf.ui.options`
- Size: 176 lines
- Component: None
- Extends: AbstractOptionElement
- Implements: OptionsPage

**OptionsPage**
- Package: `org.jppf.ui.options`
- Size: 43 lines
- Component: None
- Extends: OptionElement

**PasswordOption**
- Package: `org.jppf.ui.options`
- Size: 73 lines
- Component: None
- Extends: TextOption

**PlainTextOption**
- Package: `org.jppf.ui.options`
- Size: 96 lines
- Component: None
- Extends: TextOption

**encapsulates**
- Package: `org.jppf.server.nio.classloader`
- Size: 49 lines
- Component: None

**AbstractJPPFTreeTableModel**
- Package: `org.jppf.ui.treetable`
- Size: 148 lines
- Component: None
- Extends: AbstractTreeTableModel

**AbstractTreeCellRenderer**
- Package: `org.jppf.ui.treetable`
- Size: 166 lines
- Component: None
- Extends: DefaultTreeCellRenderer

**TreeTableModel**
- Package: `org.jppf.ui.treetable`
- Size: 108 lines
- Component: None
- Extends: TreeModel

**takes**
- Package: `org.jppf.ui.treetable`
- Size: 327 lines
- Component: None

**handles**
- Package: `org.jppf.server.node`
- Size: 156 lines
- Component: None

**hold**
- Package: `org.jppf.server.node`
- Size: 41 lines
- Component: None

**StatsHandlerEvent**
- Package: `org.jppf.ui.monitoring.event`
- Size: 77 lines
- Component: None
- Extends: EventObject

**StatsHandlerListener**
- Package: `org.jppf.ui.monitoring.event`
- Size: 33 lines
- Component: None
- Extends: EventListener

**JobDataPanel**
- Package: `org.jppf.ui.monitoring.job`
- Size: 369 lines
- Component: None
- Extends: AbstractTreeTableOption
- Implements: ClientListener, ActionHolder

**JobDataPanelActionManager**
- Package: `org.jppf.ui.monitoring.job`
- Size: 38 lines
- Component: None
- Extends: JTreeTableActionHandler

**manages**
- Package: `org.jppf.ui.monitoring.charts.config`
- Size: 74 lines
- Component: None

**JobNotificationListener**
- Package: `org.jppf.ui.monitoring.job`
- Size: 88 lines
- Component: None
- Implements: NotificationListener

**JobRenderer**
- Package: `org.jppf.ui.monitoring.job`
- Size: 100 lines
- Component: None
- Extends: AbstractTreeCellRenderer

**JobTableCellRenderer**
- Package: `org.jppf.ui.monitoring.job`
- Size: 65 lines
- Component: None
- Extends: DefaultTableCellRenderer

**JobTreeTableModel**
- Package: `org.jppf.ui.monitoring.job`
- Size: 165 lines
- Component: None
- Extends: AbstractJPPFTreeTableModel
- Implements: TreeTableModel

**JobTreeTableMouseListener**
- Package: `org.jppf.ui.monitoring.job`
- Size: 109 lines
- Component: None
- Extends: MouseAdapter

**HTMLPropertiesTableFormat**
- Package: `org.jppf.ui.monitoring.node`
- Size: 120 lines
- Component: None
- Extends: PropertiesTableFormat

**JPPFNodeTreeTableModel**
- Package: `org.jppf.ui.monitoring.node`
- Size: 164 lines
- Component: None
- Extends: AbstractJPPFTreeTableModel

**NodeDataPanel**
- Package: `org.jppf.ui.monitoring.node`
- Size: 458 lines
- Component: None
- Extends: AbstractTreeTableOption
- Implements: ClientListener, ActionHolder

**NodeRenderer**
- Package: `org.jppf.ui.monitoring.node`
- Size: 121 lines
- Component: None
- Extends: AbstractTreeCellRenderer

**TextPropertiesTableFormat**
- Package: `org.jppf.ui.monitoring.node`
- Size: 69 lines
- Component: None
- Extends: PropertiesTableFormat

**AbstractSuspendJobAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 100 lines
- Component: None
- Extends: AbstractJobAction

**ResumeJobAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 98 lines
- Component: None
- Extends: AbstractJobAction

**SuspendJobAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 46 lines
- Component: None
- Extends: AbstractSuspendJobAction

**SuspendRequeueJobAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 46 lines
- Component: None
- Extends: AbstractSuspendJobAction

**UpdateMaxNodesAction**
- Package: `org.jppf.ui.monitoring.job.actions`
- Size: 161 lines
- Component: None
- Extends: AbstractJobAction

**NodeConfigurationAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 202 lines
- Component: None
- Extends: AbstractTopologyAction

**NodeThreadsAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 167 lines
- Component: None
- Extends: AbstractTopologyAction

**ResetTaskCounterAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 81 lines
- Component: None
- Extends: AbstractTopologyAction

**RestartNodeAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 87 lines
- Component: None
- Extends: AbstractTopologyAction

**RestartTaskAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 87 lines
- Component: None
- Extends: JPPFAbstractNodeAction

**SelectDriversAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 60 lines
- Component: None
- Extends: AbstractSelectionAction

**SelectNodesAction**
- Package: `org.jppf.ui.monitoring.node.actions`
- Size: 70 lines
- Component: None
- Extends: AbstractSelectionAction

**DebugMouseListener**
- Package: `org.jppf.ui.options.xml`
- Size: 117 lines
- Component: None
- Extends: MouseAdapter

**build**
- Package: `org.jppf.node.policy`
- Size: 376 lines
- Component: None

**loader**
- Package: `org.jppf.server.nio`
- Size: 190 lines
- Component: None

**server**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 48 lines
- Component: None

**ClientConnectionHandler**
- Package: `org.jppf.client`
- Size: 49 lines
- Component: None
- Extends: ClientConnectionStatusHandler

**JPPFClientConnection**
- Package: `org.jppf.client`
- Size: 60 lines
- Component: None
- Extends: ClientConnectionStatusHandler

**JPPFResultCollector**
- Package: `org.jppf.client`
- Size: 132 lines
- Component: None
- Implements: TaskResultListener

**handle**
- Package: `org.jppf.comm.discovery`
- Size: 132 lines
- Component: None

**FutureResultCollector**
- Package: `org.jppf.client.concurrent`
- Size: 175 lines
- Component: None
- Extends: JPPFResultCollector

**FutureResultCollectorEvent**
- Package: `org.jppf.client.concurrent`
- Size: 46 lines
- Component: None
- Extends: EventObject

**JPPFTaskFuture**
- Package: `org.jppf.client.concurrent`
- Size: 164 lines
- Component: None

**RunnableWrapper**
- Package: `org.jppf.client.concurrent`
- Size: 64 lines
- Component: None

**ClientConnectionStatusEvent**
- Package: `org.jppf.client.event`
- Size: 64 lines
- Component: None
- Extends: EventObject

**ClientConnectionStatusHandler**
- Package: `org.jppf.client.event`
- Size: 53 lines
- Component: None

**listen**
- Package: `org.jppf.server.app`
- Size: 191 lines
- Component: None

**ClientListener**
- Package: `org.jppf.client.event`
- Size: 34 lines
- Component: None
- Extends: EventListener

**TaskResultEvent**
- Package: `org.jppf.client.event`
- Size: 79 lines
- Component: None
- Extends: EventObject

**ClientProportionalBundler**
- Package: `org.jppf.client.loadbalancer`
- Size: 83 lines
- Component: None
- Extends: AbstractProportionalBundler

**TaskWrapper**
- Package: `org.jppf.client.loadbalancer`
- Size: 58 lines
- Component: None
- Implements: Runnable

**TestJPPFConfiguration**
- Package: `org.jppf.utils`
- Size: 73 lines
- Component: None

**TestJPPFJobSLA**
- Package: `org.jppf.server.protocol`
- Size: 169 lines
- Component: None
- Extends: Setup1D1N1C

**TestJPPFTask**
- Package: `org.jppf.server.protocol`
- Size: 127 lines
- Component: None
- Extends: Setup1D1N1C

**TestDriverJobManagementMBean**
- Package: `org.jppf.server.job.management`
- Size: 113 lines
- Component: None
- Extends: Setup1D1N1C

**DriverProcessLauncher**
- Package: `org.jppf.test.setup`
- Size: 41 lines
- Component: None
- Extends: GenericProcessLauncher

**LifeCycleTask**
- Package: `org.jppf.test.setup`
- Size: 160 lines
- Component: None
- Extends: JPPFTask

**NodeProcessLauncher**
- Package: `org.jppf.test.setup`
- Size: 40 lines
- Component: None
- Extends: GenericProcessLauncher

**SimpleCallable**
- Package: `org.jppf.test.setup`
- Size: 92 lines
- Component: None
- Implements: Callable<Result>, Serializable

**SimpleRunnable**
- Package: `org.jppf.test.setup`
- Size: 61 lines
- Component: None
- Implements: Runnable, Serializable

**SimpleTask**
- Package: `org.jppf.test.setup`
- Size: 71 lines
- Component: None
- Extends: JPPFTask

**AbstractFileFilter**
- Package: `org.jppf.doc`
- Size: 85 lines
- Component: None
- Implements: FileFilter

**generates**
- Package: `org.jppf.doc`
- Size: 342 lines
- Component: None

**JPPFDirFilter**
- Package: `org.jppf.doc`
- Size: 79 lines
- Component: None
- Extends: AbstractFileFilter

**JPPFFileFilter**
- Package: `org.jppf.doc`
- Size: 76 lines
- Component: None
- Extends: AbstractFileFilter

**ByteBufferInputStream**
- Package: `org.jppf.io`
- Size: 169 lines
- Component: None
- Extends: InputStream

**ByteBufferOutputStream**
- Package: `org.jppf.io`
- Size: 124 lines
- Component: None
- Extends: OutputStream

**FileInputSource**
- Package: `org.jppf.io`
- Size: 58 lines
- Component: None
- Extends: ChannelInputSource

**SocketWrapperInputSource**
- Package: `org.jppf.io`
- Size: 109 lines
- Component: None
- Implements: InputSource

**StreamInputSource**
- Package: `org.jppf.io`
- Size: 118 lines
- Component: None
- Implements: InputSource

**group**
- Package: `org.jppf.server.protocol`
- Size: 492 lines
- Component: None

**TaskExecutionNotification**
- Package: `org.jppf.management`
- Size: 54 lines
- Component: None
- Extends: Notification

**encapsulate**
- Package: `org.jppf.server.node.remote`
- Size: 175 lines
- Component: None

**manage**
- Package: `org.jppf.server.node`
- Size: 555 lines
- Component: None

**contain**
- Package: `org.jppf.scheduling`
- Size: 200 lines
- Component: None

**GroovyScriptRunner**
- Package: `org.jppf.scripting`
- Size: 98 lines
- Component: None
- Implements: ScriptRunner

**JPPFScriptingException**
- Package: `org.jppf.scripting`
- Size: 55 lines
- Component: None
- Extends: JPPFException

**RhinoScriptRunner**
- Package: `org.jppf.scripting`
- Size: 233 lines
- Component: None
- Implements: ScriptRunner

**ScriptRunnerFactory**
- Package: `org.jppf.scripting`
- Size: 45 lines
- Component: None

**JPPFCallable**
- Package: `org.jppf.utils`
- Size: 31 lines
- Component: None

**SynchronizedTask**
- Package: `org.jppf.utils`
- Size: 73 lines
- Component: None
- Implements: Runnable

**CallableTaskWrapper**
- Package: `org.jppf.client.taskwrapper`
- Size: 63 lines
- Component: None
- Extends: AbstractTaskObjectWrapper

**PojoTaskWrapper**
- Package: `org.jppf.client.taskwrapper`
- Size: 114 lines
- Component: None
- Extends: AbstractTaskObjectWrapper

**PrivilegedConstructorAction**
- Package: `org.jppf.client.taskwrapper`
- Size: 61 lines
- Component: None
- Extends: AbstractPrivilegedAction

**PrivilegedMethodAction**
- Package: `org.jppf.client.taskwrapper`
- Size: 67 lines
- Component: None
- Extends: AbstractPrivilegedAction

**RunnableTaskWrapper**
- Package: `org.jppf.client.taskwrapper`
- Size: 66 lines
- Component: None
- Extends: AbstractTaskObjectWrapper

**SocketChannelClient**
- Package: `org.jppf.comm.socket`
- Size: 443 lines
- Component: None
- Implements: SocketWrapper

**NodeLifeCycleListener**
- Package: `org.jppf.node.event`
- Size: 52 lines
- Component: None
- Extends: EventListener

**act**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 63 lines
- Component: None

**perform**
- Package: `org.jppf.server.nio`
- Size: 93 lines
- Component: None

**provide**
- Package: `org.jppf.task.storage`
- Size: 45 lines
- Component: None

**NioState**
- Package: `org.jppf.server.nio`
- Size: 36 lines
- Component: None

**SimpleNioContext**
- Package: `org.jppf.server.nio`
- Size: 91 lines
- Component: None

**wraps**
- Package: `org.jppf.server.protocol`
- Size: 160 lines
- Component: None

**FileLocation**
- Package: `org.jppf.server.protocol`
- Size: 79 lines
- Component: None
- Extends: AbstractLocation

**Location**
- Package: `org.jppf.server.protocol`
- Size: 78 lines
- Component: None

**LocationEventListener**
- Package: `org.jppf.server.protocol`
- Size: 35 lines
- Component: None
- Extends: EventListener

**MemoryLocation**
- Package: `org.jppf.server.protocol`
- Size: 115 lines
- Component: None
- Extends: AbstractLocation

**TaskCompletionListener**
- Package: `org.jppf.server.protocol`
- Size: 33 lines
- Component: None

**URLLocation**
- Package: `org.jppf.server.protocol`
- Size: 82 lines
- Component: None
- Extends: AbstractLocation

**DriverJobManagementMBean**
- Package: `org.jppf.server.job.management`
- Size: 79 lines
- Component: None
- Extends: NotificationEmitter

**NodeJobInformation**
- Package: `org.jppf.server.job.management`
- Size: 55 lines
- Component: None
- Implements: Serializable

**IdentifyingInboundChannelState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 76 lines
- Component: None
- Extends: MultiplexerServerState

**IdleState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 61 lines
- Component: None
- Extends: MultiplexerServerState

**MultiplexerContext**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 310 lines
- Component: None
- Extends: SimpleNioContext

**MultiplexerNioServer**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 223 lines
- Component: None
- Extends: NioServer

**ReceivingState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 79 lines
- Component: None
- Extends: MultiplexerServerState

**SendingMultiplexingInfoState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 83 lines
- Component: None
- Extends: MultiplexerServerState

**SendingOrReceivingState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 64 lines
- Component: None
- Extends: MultiplexerServerState

**SendingState**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 72 lines
- Component: None
- Extends: MultiplexerServerState

**ByteBufferWrapper**
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 48 lines
- Component: None

**AbstractBundler**
- Package: `org.jppf.server.scheduler.bundle`
- Size: 132 lines
- Component: None
- Implements: Bundler

**acts**
- Package: `org.jppf.server.scheduler.bundle`
- Size: 174 lines
- Component: None

**if**
- Package: `org.jppf.server.scheduler.bundle`
- Size: 39 lines
- Component: None

**LoadBalancingInformation**
- Package: `org.jppf.server.scheduler.bundle`
- Size: 68 lines
- Component: None
- Implements: Serializable

**defines**
- Package: `org.jppf.server.node`
- Size: 48 lines
- Component: None

**AbstractAutoTuneProfile**
- Package: `org.jppf.server.scheduler.bundle.autotuned`
- Size: 31 lines
- Component: None
- Implements: LoadBalancingProfile

**implements**
- Package: `org.jppf.server.scheduler.bundle.impl`
- Size: 205 lines
- Component: None

**FixedSizeProfile**
- Package: `org.jppf.server.scheduler.bundle.fixedsize`
- Size: 80 lines
- Component: None
- Implements: LoadBalancingProfile

**ProportionalTuneProfile**
- Package: `org.jppf.server.scheduler.bundle.proportional`
- Size: 154 lines
- Component: None
- Extends: AbstractAutoTuneProfile

**AbstractRLBundler**
- Package: `org.jppf.server.scheduler.bundle.rl`
- Size: 202 lines
- Component: None
- Extends: AbstractBundler

**RLProfile**
- Package: `org.jppf.server.scheduler.bundle.rl`
- Size: 142 lines
- Component: None
- Extends: AbstractAutoTuneProfile

**shall**
- Package: `org.jppf.server.scheduler.bundle.spi`
- Size: 61 lines
- Component: None

**ClientDataProvider**
- Package: `org.jppf.task.storage`
- Size: 104 lines
- Component: None
- Extends: MemoryMapDataProvider

**MemoryMapDataProvider**
- Package: `org.jppf.task.storage`
- Size: 55 lines
- Component: None
- Implements: DataProvider

**LoggingRunner**
- Package: `org.jppf.example.jmxlogger.test`
- Size: 243 lines
- Component: None
- Implements: NotificationListener

**LoggingTask**
- Package: `org.jppf.example.jmxlogger.test`
- Size: 61 lines
- Component: None
- Extends: JPPFTask

**AllInOne**
- Package: `sample.allinone`
- Size: 64 lines
- Component: None

**Task1**
- Package: `sample.cascading`
- Size: 62 lines
- Component: None
- Extends: JPPFTask

**Task2**
- Package: `sample.cascading`
- Size: 39 lines
- Component: None
- Extends: JPPFTask

**DataProviderTestTask**
- Package: `sample.clientdataprovider`
- Size: 106 lines
- Component: None
- Extends: JPPFTask

**MyCallable**
- Package: `sample.datasize`
- Size: 51 lines
- Component: None
- Implements: Callable<String>, Serializable

**HelloWorld**
- Package: `sample.helloworld`
- Size: 39 lines
- Component: None
- Extends: JPPFTask

**HelloWorldAnnotated**
- Package: `sample.helloworld`
- Size: 47 lines
- Component: None
- Implements: Serializable

**HelloWorldAnnotatedConstructor**
- Package: `sample.helloworld`
- Size: 59 lines
- Component: None
- Implements: Serializable

**HelloWorldAnnotatedStatic**
- Package: `sample.helloworld`
- Size: 47 lines
- Component: None
- Implements: Serializable

**HelloWorldCallable**
- Package: `sample.helloworld`
- Size: 45 lines
- Component: None
- Implements: Callable<String>, Serializable

**HelloWorldPojo**
- Package: `sample.helloworld`
- Size: 44 lines
- Component: None
- Implements: Serializable

**HelloWorldPojoConstructor**
- Package: `sample.helloworld`
- Size: 57 lines
- Component: None
- Implements: Serializable

**HelloWorldPojoStatic**
- Package: `sample.helloworld`
- Size: 44 lines
- Component: None
- Implements: Serializable

**HelloWorldRunnable**
- Package: `sample.helloworld`
- Size: 56 lines
- Component: None
- Implements: Runnable, Serializable

**HelloWorldRunner**
- Package: `sample.helloworld`
- Size: 72 lines
- Component: None

**DriverAndNodeLauncher**
- Package: `sample.misc`
- Size: 60 lines
- Component: None

**NBody**
- Package: `sample.nbody`
- Size: 60 lines
- Component: None
- Implements: Serializable

**NBodyPanel**
- Package: `sample.nbody`
- Size: 159 lines
- Component: None
- Extends: JPanel

**NBodyTask**
- Package: `sample.nbody`
- Size: 111 lines
- Component: None
- Extends: JPPFTask

**Vector2d**
- Package: `sample.nbody`
- Size: 153 lines
- Component: None
- Implements: Serializable

**LargeInt**
- Package: `sample.prime`
- Size: 219 lines
- Component: None
- Implements: Serializable

**MBeanClient**
- Package: `sample.taskmonitor`
- Size: 108 lines
- Component: None
- Extends: JMXConnectionWrapper
- Implements: NotificationListener

**fails**
- Package: `sample.test`
- Size: 47 lines
- Component: None

**ExceptionTestTask**
- Package: `sample.test`
- Size: 57 lines
- Component: None
- Extends: JPPFTestTask

**ExecutionReport**
- Package: `sample.test`
- Size: 43 lines
- Component: None
- Implements: Serializable

**FileDownloadTestTask**
- Package: `sample.test`
- Size: 52 lines
- Component: None
- Extends: JPPFTestTask

**JPPFClient**
- Package: `sample.test`
- Size: 119 lines
- Component: None

**NonSerializableAttributeTask**
- Package: `sample.test`
- Size: 55 lines
- Component: None
- Extends: JPPFTask

**NonSerializableInterface**
- Package: `sample.test`
- Size: 31 lines
- Component: None

**OutOfMemoryTestTask**
- Package: `sample.test`
- Size: 49 lines
- Component: None
- Extends: JPPFTask

**ParserTask**
- Package: `sample.test`
- Size: 78 lines
- Component: None
- Extends: JPPFTestTask

**SimpleData**
- Package: `sample.test`
- Size: 56 lines
- Component: None
- Implements: Serializable

**TestAnnotatedStaticTask**
- Package: `sample.test`
- Size: 56 lines
- Component: None
- Implements: Serializable

**TestAnnotatedTask**
- Package: `sample.test`
- Size: 49 lines
- Component: None
- Implements: Serializable

**TestUuid**
- Package: `sample.test`
- Size: 98 lines
- Component: None

**TimeoutTask**
- Package: `sample.test`
- Size: 59 lines
- Component: None
- Extends: JPPFTestTask

**ListDirectoryTask**
- Package: `sample.dist.commandline`
- Size: 90 lines
- Component: None
- Extends: CommandLineTask

**MyCustomPolicy**
- Package: `sample.dist.matrix`
- Size: 50 lines
- Component: None
- Extends: CustomPolicy

**MyTask1**
- Package: `sample.dist.taskcommunication`
- Size: 54 lines
- Component: None
- Extends: AbstractMyTask

**MyTask2**
- Package: `sample.dist.taskcommunication`
- Size: 54 lines
- Component: None
- Extends: AbstractMyTask

**MyTaskRunner**
- Package: `sample.dist.taskcommunication`
- Size: 68 lines
- Component: None

**PoolConfigGenerator**
- Package: `sample.test.clientpool`
- Size: 74 lines
- Component: None

**do**
- Package: `sample.test.profiling`
- Size: 54 lines
- Component: None

**JobManagementTestRunner**
- Package: `sample.test.job.management`
- Size: 93 lines
- Component: None

**ExampleGridRunner**
- Package: `test.classversion`
- Size: 69 lines
- Component: None

**ExampleTask**
- Package: `test.classversion`
- Size: 41 lines
- Component: None
- Extends: JPPFTask

**TestEncryption**
- Package: `test.encryption`
- Size: 69 lines
- Component: None

**CallableTask**
- Package: `test.generic`
- Size: 51 lines
- Component: None
- Implements: Callable<String>, Serializable

**GenericRunner**
- Package: `test.generic`
- Size: 107 lines
- Component: None

**LotsOfOutputTask**
- Package: `test.generic`
- Size: 61 lines
- Component: None
- Extends: JPPFTask

**Probe**
- Package: `test.ipprobe`
- Size: 110 lines
- Component: None

**TestJMX**
- Package: `test.jmx`
- Size: 89 lines
- Component: None

**Application**
- Package: `test.nathalie`
- Size: 16 lines
- Component: None

**Beginning**
- Package: `test.nathalie`
- Size: 37 lines
- Component: None

**Execution**
- Package: `test.nathalie`
- Size: 34 lines
- Component: None

**Final**
- Package: `test.nathalie`
- Size: 21 lines
- Component: None
- Extends: JPPFTask

**Intercal**
- Package: `test.nathalie`
- Size: 9 lines
- Component: None

**Island**
- Package: `test.nathalie`
- Size: 19 lines
- Component: None
- Extends: JPPFTask

**PrioritizedTask**
- Package: `test.priority`
- Size: 51 lines
- Component: None
- Extends: JPPFTask

**PriorityTestRunner**
- Package: `test.priority`
- Size: 118 lines
- Component: None

**WaitTask**
- Package: `test.priority`
- Size: 59 lines
- Component: None
- Extends: JPPFTask

**SocketPerformance**
- Package: `test.socket`
- Size: 254 lines
- Component: None

**NativeLibLoader**
- Package: `test.node.nativelib`
- Size: 39 lines
- Component: None

**NativeLibRunner**
- Package: `test.node.nativelib`
- Size: 186 lines
- Component: None

**TimeoutTaskRunner**
- Package: `test.node.tasktimeout`
- Size: 186 lines
- Component: None

**DemoTask**
- Package: `org.jppf.jca.demo`
- Size: 65 lines
- Component: None
- Extends: JPPFTask

**DurationTask**
- Package: `org.jppf.jca.demo`
- Size: 84 lines
- Component: None
- Extends: JPPFTask

**JcaObjectSerializerImpl**
- Package: `org.jppf.jca.serialization`
- Size: 54 lines
- Component: None
- Extends: ObjectSerializerImpl

**JcaSerializationHelperImpl**
- Package: `org.jppf.jca.serialization`
- Size: 72 lines
- Component: None
- Extends: SerializationHelperImpl

**JPPFConnectionMetaData**
- Package: `org.jppf.jca.cci`
- Size: 72 lines
- Component: None
- Implements: ConnectionMetaData

**JPPFInteraction**
- Package: `org.jppf.jca.cci`
- Size: 106 lines
- Component: None
- Implements: Interaction

**JPPFInteractionSpec**
- Package: `org.jppf.jca.cci`
- Size: 87 lines
- Component: None
- Implements: InteractionSpec

**JPPFManagedConnection**
- Package: `org.jppf.jca.spi`
- Size: 200 lines
- Component: None
- Extends: JPPFAccessorImpl
- Implements: ManagedConnection

**JPPFManagedConnectionFactory**
- Package: `org.jppf.jca.spi`
- Size: 142 lines
- Component: None
- Extends: JPPFAccessorImpl
- Implements: ManagedConnectionFactory, ResourceAdapterAssociation

**JPPFManagedConnectionMetaData**
- Package: `org.jppf.jca.spi`
- Size: 88 lines
- Component: None
- Implements: ManagedConnectionMetaData

**send**
- Package: `org.jppf.logging.jmx`
- Size: 200 lines
- Component: None

**JPPFException**
- Package: `org.jppf`
- Size: 53 lines
- Component: None
- Extends: Exception

**JPPFNodeReconnectionNotification**
- Package: `org.jppf`
- Size: 53 lines
- Component: None
- Extends: JPPFError

**JPPFNodeReloadNotification**
- Package: `org.jppf`
- Size: 34 lines
- Component: None
- Extends: JPPFError

**NonDelegatingClassLoader**
- Package: `org.jppf.classloader`
- Size: 100 lines
- Component: None
- Extends: URLClassLoader

**SaveFileAction**
- Package: `org.jppf.classloader`
- Size: 95 lines
- Component: None
- Implements: PrivilegedAction<File>

**AbstractIPAddressPattern**
- Package: `org.jppf.net`
- Size: 207 lines
- Component: None

**IPv4AddressPattern**
- Package: `org.jppf.net`
- Size: 102 lines
- Component: None
- Extends: AbstractIPAddressPattern

**IPv6AddressPattern**
- Package: `org.jppf.net`
- Size: 115 lines
- Component: None
- Extends: AbstractIPAddressPattern

**isten**
- Package: `org.jppf.process`
- Size: 61 lines
- Component: None

**read**
- Package: `org.jppf.process`
- Size: 160 lines
- Component: None

**provided**
- Package: `org.jppf.security`
- Size: 41 lines
- Component: None

**JPPFPermissions**
- Package: `org.jppf.security`
- Size: 155 lines
- Component: None
- Extends: PermissionCollection

**JPPFPolicy**
- Package: `org.jppf.security`
- Size: 142 lines
- Component: None
- Extends: Policy

**JPPFConfigurationObjectStreamBuilder**
- Package: `org.jppf.serialization`
- Size: 117 lines
- Component: None
- Implements: JPPFObjectStreamBuilder

**JPPFObjectStreamBuilder**
- Package: `org.jppf.serialization`
- Size: 43 lines
- Component: None

**JPPFObjectStreamBuilderImpl**
- Package: `org.jppf.serialization`
- Size: 54 lines
- Component: None
- Implements: JPPFObjectStreamBuilder

**class**
- Package: `org.jppf.startup`
- Size: 41 lines
- Component: None

**JPPFStartupLoader**
- Package: `org.jppf.startup`
- Size: 63 lines
- Component: None

**IteratorEnumeration**
- Package: `org.jppf.utils`
- Size: 63 lines
- Component: None

**JPPFBuffer**
- Package: `org.jppf.utils`
- Size: 131 lines
- Component: None

**JPPFByteArrayOutputStream**
- Package: `org.jppf.utils`
- Size: 65 lines
- Component: None
- Extends: ByteArrayOutputStream

**JPPFThreadFactory**
- Package: `org.jppf.utils`
- Size: 157 lines
- Component: None
- Implements: ThreadFactory

**MultipleBuffersOutputStream**
- Package: `org.jppf.utils`
- Size: 214 lines
- Component: None
- Extends: OutputStream

**look**
- Package: `org.jppf.utils`
- Size: 183 lines
- Component: None

**be**
- Package: `org.jppf.utils`
- Size: 27 lines
- Component: None

**TraversalList**
- Package: `org.jppf.utils`
- Size: 167 lines
- Component: None

**ClientConnectionEvent**
- Package: `org.jppf.comm.recovery`
- Size: 46 lines
- Component: None
- Extends: EventObject

**should**
- Package: `org.jppf.comm.recovery`
- Size: 35 lines
- Component: None

**checks**
- Package: `org.jppf.comm.recovery`
- Size: 185 lines
- Component: None

**ReaperEvent**
- Package: `org.jppf.comm.recovery`
- Size: 46 lines
- Component: None
- Extends: EventObject

**JmxHandler**
- Package: `org.jppf.logging.jdk`
- Size: 75 lines
- Component: None
- Extends: Handler

**JPPFFileHandler**
- Package: `org.jppf.logging.jdk`
- Size: 52 lines
- Component: None
- Extends: FileHandler

**JPPFLogFormatter**
- Package: `org.jppf.logging.jdk`
- Size: 100 lines
- Component: None
- Extends: Formatter

**JmxLoggerImpl**
- Package: `org.jppf.logging.jmx`
- Size: 73 lines
- Component: None
- Extends: NotificationBroadcasterSupport
- Implements: JmxLogger

**JmxAppender**
- Package: `org.jppf.logging.log4j`
- Size: 117 lines
- Component: None
- Extends: AppenderSkeleton

**describe**
- Package: `org.jppf.node.event`
- Size: 69 lines
- Component: None

**IdleDetectionTask**
- Package: `org.jppf.node.idle`
- Size: 167 lines
- Component: None
- Extends: TimerTask

**detects**
- Package: `org.jppf.node.idle`
- Size: 147 lines
- Component: None

**IdleStateEvent**
- Package: `org.jppf.node.idle`
- Size: 61 lines
- Component: None
- Extends: EventObject

**IdleTimeDetector**
- Package: `org.jppf.node.idle`
- Size: 32 lines
- Component: None

**IdleTimeDetectorFactory**
- Package: `org.jppf.node.idle`
- Size: 32 lines
- Component: None

**AtLeast**
- Package: `org.jppf.node.policy`
- Size: 92 lines
- Component: None
- Extends: ExecutionPolicy

**AtMost**
- Package: `org.jppf.node.policy`
- Size: 92 lines
- Component: None
- Extends: ExecutionPolicy

**BetweenEE**
- Package: `org.jppf.node.policy`
- Size: 103 lines
- Component: None
- Extends: ExecutionPolicy

**BetweenEI**
- Package: `org.jppf.node.policy`
- Size: 103 lines
- Component: None
- Extends: ExecutionPolicy

**BetweenIE**
- Package: `org.jppf.node.policy`
- Size: 103 lines
- Component: None
- Extends: ExecutionPolicy

**BetweenII**
- Package: `org.jppf.node.policy`
- Size: 103 lines
- Component: None
- Extends: ExecutionPolicy

**Contains**
- Package: `org.jppf.node.policy`
- Size: 94 lines
- Component: None
- Extends: ExecutionPolicy

**Equal**
- Package: `org.jppf.node.policy`
- Size: 142 lines
- Component: None
- Extends: ExecutionPolicy

**LessThan**
- Package: `org.jppf.node.policy`
- Size: 92 lines
- Component: None
- Extends: ExecutionPolicy

**MoreThan**
- Package: `org.jppf.node.policy`
- Size: 92 lines
- Component: None
- Extends: ExecutionPolicy

**OneOf**
- Package: `org.jppf.node.policy`
- Size: 139 lines
- Component: None
- Extends: ExecutionPolicy

**PolicyDescriptor**
- Package: `org.jppf.node.policy`
- Size: 59 lines
- Component: None

**JPPFScreenSaver**
- Package: `org.jppf.node.screensaver`
- Size: 488 lines
- Component: None

**AbstractChannelWrapper**
- Package: `org.jppf.server.nio`
- Size: 218 lines
- Component: None

**AbstractNioContext**
- Package: `org.jppf.server.nio`
- Size: 139 lines
- Component: None

**ChannelSelector**
- Package: `org.jppf.server.nio`
- Size: 53 lines
- Component: None

**ChannelWrapper**
- Package: `org.jppf.server.nio`
- Size: 105 lines
- Component: None

**NioContext**
- Package: `org.jppf.server.nio`
- Size: 86 lines
- Component: None

**JPPFNodeTaskMonitor**
- Package: `org.jppf.management`
- Size: 152 lines
- Component: None
- Extends: NotificationBroadcasterSupport
- Implements: JPPFNodeTaskMonitorMBean, TaskExecutionListener

**ChannelContext**
- Package: `org.jppf.server`
- Size: 41 lines
- Component: None

**ShutdownRestartTask**
- Package: `org.jppf.server`
- Size: 101 lines
- Component: None
- Extends: TimerTask

**associate**
- Package: `org.jppf.server.job`
- Size: 56 lines
- Component: None

**NodeTaskWrapper**
- Package: `org.jppf.server.node`
- Size: 129 lines
- Component: None
- Implements: Runnable

**JPPFPriorityQueue**
- Package: `org.jppf.server.queue`
- Size: 526 lines
- Component: None
- Extends: AbstractJPPFQueue

**JPPFQueue**
- Package: `org.jppf.server.queue`
- Size: 68 lines
- Component: None
- Extends: Iterable

**QueueListener**
- Package: `org.jppf.server.queue`
- Size: 34 lines
- Component: None
- Extends: EventListener

**JPPFDefaultDriverMBeanProvider**
- Package: `org.jppf.server.spi`
- Size: 62 lines
- Component: None
- Implements: JPPFDriverMBeanProvider

**DriverJobManagement**
- Package: `org.jppf.server.job.management`
- Size: 288 lines
- Component: None
- Extends: NotificationBroadcasterSupport
- Implements: DriverJobManagementMBean

**DriverJobManagementMBeanProvider**
- Package: `org.jppf.server.job.management`
- Size: 62 lines
- Component: None
- Implements: JPPFDriverMBeanProvider

**SendingNodeInitialResponseState**
- Package: `org.jppf.server.nio.classloader`
- Size: 75 lines
- Component: None
- Extends: ClassServerState

**SendingProviderInitialResponseState**
- Package: `org.jppf.server.nio.classloader`
- Size: 75 lines
- Component: None
- Extends: ClassServerState

**AbstractNodeContext**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 280 lines
- Component: None
- Extends: AbstractNioContext

**LocalNodeChannel**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 37 lines
- Component: None
- Extends: AbstractLocalChannelWrapper

**LocalNodeContext**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 94 lines
- Component: None
- Extends: AbstractNodeContext

**LocalNodeMessage**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 72 lines
- Component: None
- Extends: AbstractNodeMessage

**RemoteNodeContext**
- Package: `org.jppf.server.nio.nodeserver`
- Size: 36 lines
- Component: None
- Extends: AbstractNodeContext

**JPPFLocalNode**
- Package: `org.jppf.server.node.local`
- Size: 110 lines
- Component: None
- Extends: JPPFNode

**JPPFDefaultNodeMBeanProvider**
- Package: `org.jppf.server.node.spi`
- Size: 65 lines
- Component: None
- Implements: JPPFNodeMBeanProvider

**JPPFNodeTaskMonitorProvider**
- Package: `org.jppf.server.node.spi`
- Size: 67 lines
- Component: None
- Implements: JPPFNodeMBeanProvider

**AutotunedDelegatingBundler**
- Package: `org.jppf.server.scheduler.bundle.impl`
- Size: 123 lines
- Component: None
- Extends: AbstractBundler

**NodeSimulator**
- Package: `org.jppf.server.scheduler.bundle.impl`
- Size: 267 lines
- Component: None

**RLBundler**
- Package: `org.jppf.server.scheduler.bundle.impl`
- Size: 77 lines
- Component: None
- Extends: AbstractRLBundler

**AutoTunedBundlerProvider**
- Package: `org.jppf.server.scheduler.bundle.providers`
- Size: 64 lines
- Component: None
- Implements: JPPFBundlerProvider

**FixedSizeBundlerProvider**
- Package: `org.jppf.server.scheduler.bundle.providers`
- Size: 63 lines
- Component: None
- Implements: JPPFBundlerProvider

**ProportionalBundlerProvider**
- Package: `org.jppf.server.scheduler.bundle.providers`
- Size: 66 lines
- Component: None
- Implements: JPPFBundlerProvider

**RLBundlerProvider**
- Package: `org.jppf.server.scheduler.bundle.providers`
- Size: 65 lines
- Component: None
- Implements: JPPFBundlerProvider

**Asian**
- Package: `None`
- Size: 74 lines
- Component: None

**AsianQMC**
- Package: `None`
- Size: 66 lines
- Component: None
- Extends: Asian

**AsianQMCk**
- Package: `None`
- Size: 67 lines
- Component: None
- Extends: AsianQMC

**BankEv**
- Package: `None`
- Size: 114 lines
- Component: None

**BankProc**
- Package: `None`
- Size: 88 lines
- Component: None

**Collision**
- Package: `None`
- Size: 43 lines
- Component: None

**Inventory**
- Package: `None`
- Size: 63 lines
- Component: None

**InventoryCRN**
- Package: `None`
- Size: 54 lines
- Component: None
- Extends: Inventory

**Nonuniform**
- Package: `None`
- Size: 46 lines
- Component: None

**QueueLindley**
- Package: `None`
- Size: 37 lines
- Component: None

**QueueObs**
- Package: `None`
- Size: 76 lines
- Component: None

**QueueObs2**
- Package: `None`
- Size: 83 lines
- Component: None

## Model Component Mapping

- **Accessibility:** 3 files
- **Individual Tour:** 9 files
- **Joint Tour:** 1 files
- **Main Runner:** 2 files
- **Mandatory Tour:** 85 files
- **Parking:** 2 files
- **Population Synthesis:** 94 files
- **Stop Frequency:** 52 files
- **Stop Location:** 19 files
- **Tour Mode Choice:** 3 files
- **Tour Tod:** 2 files
- **Trip Mode:** 2 files

## UEC File Cross-Reference

Based on Step 2 inventory and Java analysis:

| Excel UEC File | Likely Java Processor | Component |
|----------------|---------------------|-----------|
| TourModeChoice.xls | TourModeChoiceModel | tour_mode_choice |
| TourDestinationChoice.xls | *TourDestinationChoiceModel (not found)* | tour_destination |
| MandatoryTourFrequency.xls | *MandatoryTourFreqModel (not found)* | mandatory_tour |
| NonMandatoryIndividualTourFrequency.xls | *NonMandatoryTourFreqModel (not found)* | individual_tour |
| JointTourFrequency.xls | *JointTourFreqModel (not found)* | joint_tour |
| StopFrequency.xls | *StopFrequencyModel (not found)* | stop_frequency |
| AutoOwnership.xls | *AutoOwnershipModel (not found)* | auto_ownership |

## Next Steps for Step 4

Key Java files identified for detailed UEC mapping:
1. **UEC Processor Classes** - Files that read and process .xls UEC files
2. **Model Runner Classes** - Files that orchestrate model execution
3. **Data Manager Classes** - Files that handle input/output data
4. **Choice Model Classes** - Files implementing discrete choice models

---
*This analysis provides the foundation for mapping UEC files to Java implementation in Step 4.*
