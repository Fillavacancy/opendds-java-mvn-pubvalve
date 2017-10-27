import DDS.*;
import Nexmatix.*;
import OpenDDS.DCPS.DEFAULT_STATUS_MASK;
import OpenDDS.DCPS.TheParticipantFactory;
import OpenDDS.DCPS.TheServiceParticipant;
import org.omg.CORBA.StringSeqHolder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;


public class PubValve {

    // place native opendds dynamic libs in project current working directory
    static {
        try {
            final String osName = System.getProperty("os.name").toLowerCase();
            String nativeJarName = null;
            if (osName.contains("mac")) {
                nativeJarName = "OpenDDSDarwin.jar";
            } else if (osName.contains("linux")) {
                nativeJarName = "OpenDDSLinux.jar";
            } else if (osName.contains("Windows")) {
                nativeJarName = "OpenDDSWindows.jar";
            }

            if (nativeJarName == null) {
                throw new UnsupportedOperationException("No known OpenDDS native jar for OS " + osName);
            } else {
                System.out.println(nativeJarName + " contains native libaries for " + osName);
            }

            final String currentWorkingDirString = Paths.get("").toAbsolutePath().normalize().toString();
            final Path jarFilePath = Paths.get(currentWorkingDirString, nativeJarName);
            Files.deleteIfExists(jarFilePath);
            final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(nativeJarName);
            Files.copy(stream, jarFilePath);
            stream.close();

            if (Files.exists(jarFilePath)) {
                // unpack Jar to cwd
                Map<String, String> libFileNameMap = new HashMap<>();
                JarFile jar = new JarFile(jarFilePath.toString());
                for (Enumeration<JarEntry> enumEntries = jar.entries(); enumEntries.hasMoreElements(); ) {
                    final JarEntry entry = enumEntries.nextElement();
                    final int startIndex = 0;
                    final int endIndex = entry.getName().indexOf(".");
                    final String key = entry.getName().substring(startIndex, endIndex).toLowerCase();
                    final Path dynamicLibPath = Paths.get(currentWorkingDirString, entry.getName());
                    libFileNameMap.put(key, entry.getName());
                    Files.deleteIfExists(dynamicLibPath);
                    Files.copy(jar.getInputStream(entry), dynamicLibPath);
                }
                jar.close();

                //libFileNameMap.forEach((id, val) -> System.out.println(id + ":" + val));

                // load dynamic libraries with path to current directory to
                // support rpath location mechanism to resolve to unpacked library path
                final String libs[] = {
                        "libACE",
                        "libTAO",
                        "libTAO_AnyTypeCode",
                        "libTAO_PortableServer",
                        "libTAO_CodecFactory",
                        "libTAO_PI",
                        "libTAO_BiDirGIOP",
                        "libidl2jni_runtime",
                        "libtao_java",
                        "libOpenDDS_Dcps",
                        "libOpenDDS_Udp",
                        "libOpenDDS_Tcp",
                        "libOpenDDS_Rtps",
                        "libOpenDDS_Rtps_Udp",
                        "libOpenDDS_DCPS_Java"
                };
                for (String lib : libs) {
                    final String key = lib.toLowerCase();
                    if (libFileNameMap.containsKey(key)) {
                        final String libFileName = libFileNameMap.get(key);
                        System.out.println("Loading: " + libFileName);
                        System.load(Paths.get(currentWorkingDirString, libFileName).toAbsolutePath().normalize().toString());
                    } else {
                        System.out.println("Skipping: " + lib);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final int N_MSGS = 40;
    private static final int VALVE_PARTICIPANT = 23;
    private static final String VALVE_TOPIC = "Valve";

    private static boolean checkReliable(String[] args) {
        for (String arg : args)
            if (arg.equals("-r")) {
                return true;
            }
        return false;
    }

    private static boolean checkWaitForAcks(String[] args) {
        for (String arg : args) {
            if (arg.equals("-w")) {
                return true;
            }
        }
        return false;
    }

    private static final int max_manifold_id = 5;
    private static final int valve_count = 6;

    private static int serial_number(int valve_id, int manifold_id) {
        return valve_id * 1000000 + 10010 + manifold_id;
    }

    private static String part_number(int valve_id, int manifold_id) {
        //std::snprintf(buf, 64, "%d%dNX-DCV-SM-BLU-1-1-VO-L1-SO-OO", valve_id, manifold_id);
        StringBuilder sb = new StringBuilder(64);
        sb.append(valve_id);
        sb.append(manifold_id);
        sb.append("NX-DCV-SM-BLU-1-1-VO-L1-SO-OO");
        return sb.toString();
    }

    private static int lifecycle_count(int valve_id, int manifold_id) {
        int base = 1;
        for (int i = 0; i < manifold_id / 2; ++i) {
            base *= 10;
        }
        return base + valve_id;
    }

    private static void printValveData(ValveData valveData) {
        System.out.println("manifoldId:" + valveData.manifoldId);
        System.out.println("stationId:" + valveData.stationId);
        System.out.println("valveSerialId:" + valveData.valveSerialId);
        System.out.println("partNumber:" + valveData.partNumber);
        System.out.println("leakFault:" + valveData.leakFault);
        System.out.println("pressureFault:" + valveData.pressureFault.value());
        System.out.println("cycles:" + valveData.cycles);
        System.out.println("pressure:" + valveData.pressure);
        System.out.println("durationLast12:" + valveData.durationLast12);
        System.out.println("durationLast14:" + valveData.durationLast14);
        System.out.println("equalizationAveragePressure:" + valveData.equalizationAveragePressure);
        System.out.println("residualOfDynamicAnalysis:" + valveData.residualOfDynamicAnalysis);
        System.out.println("suppliedPressure:" + valveData.suppliedPressure);
    }

    private static final int pressureFaultArray[] = {0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 2, 2, 2, 2, 2, 0};
    private static final int suppliedPressureArray[] = {100, 90, 80, 70, 60, 65, 70, 75, 80, 80, 80, 80, 80, 80, 85, 95};
    private static final int pressureArray[] = {90, 80, 70, 60, 50, 30, 25, 25, 35, 40, 50, 70, 70, 70, 70, 85};
    private static final boolean leakFaultArray[] = {false, false, false, true, true, true, true, true, true, true, true, false, false, false, false, false};
    private static final boolean valveFaultArray[] = {false, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false};

    public static void main(String[] args) {

        System.out.println("Start Valve Publisher");
        boolean reliable = checkReliable(args);
        //boolean waitForAcks = checkWaitForAcks(args);
        int tick_ = 0;

        DomainParticipantFactory domainParticipantFactory = TheParticipantFactory.WithArgs(new StringSeqHolder(args));
        if (domainParticipantFactory == null) {
            System.err.println("ERROR: Domain Participant Factory not found");
            return;
        }
        DomainParticipant domainParticipant = domainParticipantFactory.create_participant(VALVE_PARTICIPANT,
                PARTICIPANT_QOS_DEFAULT.get(),
                null,
                DEFAULT_STATUS_MASK.value);
        if (domainParticipant == null) {
            System.err.println("ERROR: Domain Participant creation failed");
            return;
        }

        ValveDataTypeSupportImpl valveDataTypeSupport = new ValveDataTypeSupportImpl();
        if (valveDataTypeSupport.register_type(domainParticipant, "") != RETCODE_OK.value) {
            System.err.println("ERROR: register_type failed");
            return;
        }

        Topic topic = domainParticipant.create_topic(VALVE_TOPIC,
                valveDataTypeSupport.get_type_name(),
                TOPIC_QOS_DEFAULT.get(),
                null,
                DEFAULT_STATUS_MASK.value);
        if (topic == null) {
            System.err.println("ERROR: Topic creation failed");
            return;
        }

        Publisher publisher = domainParticipant.create_publisher(PUBLISHER_QOS_DEFAULT.get(),
                null,
                DEFAULT_STATUS_MASK.value);
        if (publisher == null) {
            System.err.println("ERROR: Publisher creation failed");
            return;
        }

        // Use the default transport configuration (do nothing)

        DataWriterQos dataWriterQos = new DataWriterQos();
        dataWriterQos.durability = new DurabilityQosPolicy();
        dataWriterQos.durability.kind = DurabilityQosPolicyKind.from_int(0);
        dataWriterQos.durability_service = new DurabilityServiceQosPolicy();
        dataWriterQos.durability_service.history_kind = HistoryQosPolicyKind.from_int(0);
        dataWriterQos.durability_service.service_cleanup_delay = new Duration_t();
        dataWriterQos.deadline = new DeadlineQosPolicy();
        dataWriterQos.deadline.period = new Duration_t();
        dataWriterQos.latency_budget = new LatencyBudgetQosPolicy();
        dataWriterQos.latency_budget.duration = new Duration_t();
        dataWriterQos.liveliness = new LivelinessQosPolicy();
        dataWriterQos.liveliness.kind = LivelinessQosPolicyKind.from_int(0);
        dataWriterQos.liveliness.lease_duration = new Duration_t();
        dataWriterQos.reliability = new ReliabilityQosPolicy();
        dataWriterQos.reliability.kind = ReliabilityQosPolicyKind.from_int(0);
        dataWriterQos.reliability.max_blocking_time = new Duration_t();
        dataWriterQos.destination_order = new DestinationOrderQosPolicy();
        dataWriterQos.destination_order.kind = DestinationOrderQosPolicyKind.from_int(0);
        dataWriterQos.history = new HistoryQosPolicy();
        dataWriterQos.history.kind = HistoryQosPolicyKind.from_int(0);
        dataWriterQos.resource_limits = new ResourceLimitsQosPolicy();
        dataWriterQos.transport_priority = new TransportPriorityQosPolicy();
        dataWriterQos.lifespan = new LifespanQosPolicy();
        dataWriterQos.lifespan.duration = new Duration_t();
        dataWriterQos.user_data = new UserDataQosPolicy();
        dataWriterQos.user_data.value = new byte[0];
        dataWriterQos.ownership = new OwnershipQosPolicy();
        dataWriterQos.ownership.kind = OwnershipQosPolicyKind.from_int(0);
        dataWriterQos.ownership_strength = new OwnershipStrengthQosPolicy();
        dataWriterQos.writer_data_lifecycle = new WriterDataLifecycleQosPolicy();

        DataWriterQosHolder dataWriterQosHolder = new DataWriterQosHolder(dataWriterQos);
        publisher.get_default_datawriter_qos(dataWriterQosHolder);
        dataWriterQosHolder.value.history.kind = HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS;
        if (reliable) {
            dataWriterQosHolder.value.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        }
        dataWriterQosHolder.value.durability.kind = DurabilityQosPolicyKind.TRANSIENT_LOCAL_DURABILITY_QOS;
        DataWriter dataWriter = publisher.create_datawriter(topic,
                dataWriterQosHolder.value,
                null,
                DEFAULT_STATUS_MASK.value);
        if (dataWriter == null) {
            System.err.println("ERROR: DataWriter creation failed");
            return;
        }
        System.out.println("Publisher Created DataWriter");

        StatusCondition statuscondition = dataWriter.get_statuscondition();
        statuscondition.set_enabled_statuses(PUBLICATION_MATCHED_STATUS.value);
        WaitSet waitSet = new WaitSet();
        waitSet.attach_condition(statuscondition);
        PublicationMatchedStatusHolder matched = new PublicationMatchedStatusHolder(new PublicationMatchedStatus());
        Duration_t timeout = new Duration_t(DURATION_INFINITE_SEC.value, DURATION_INFINITE_NSEC.value);

        while (true) {
            final int result = dataWriter.get_publication_matched_status(matched);
            if (result != RETCODE_OK.value) {
                System.err.println("ERROR: get_publication_matched_status()" + "failed.");
                return;
            }

            if (matched.value.current_count >= 1) {
                System.out.println("Publisher Matched");
                break;
            }

            ConditionSeqHolder conditionSeqHolder = new ConditionSeqHolder(new Condition[]{});
            if (waitSet.wait(conditionSeqHolder, timeout) != RETCODE_OK.value) {
                System.err.println("ERROR: wait() failed.");
                return;
            }
        }

        waitSet.detach_condition(statuscondition);

        ValveDataDataWriter valveDataDataWriter = ValveDataDataWriterHelper.narrow(dataWriter);
        if (valveDataDataWriter == null) {
            System.err.println("ERROR: write: narrow failed.");
            return;
        }
        boolean run_rc = true;
        while (run_rc == true) {

            int num_data = 0;

            for (int manifold_id = 1; manifold_id <= max_manifold_id; ++manifold_id) {
                for (int valve_id = 0; valve_id < valve_count; ++valve_id) {
                    ValveData valveData = new ValveData();
                    /*
                        pressure:25
                        durationLast12:0
                        durationLast14:1941973408
                        equalizationAveragePressure:32767
                        residualOfDynamicAnalysis:32767
                        suppliedPressure:75
                        manifoldId:5
                        stationId:1
                        valveSerialId:1010015
                        partNumber:15NX-DCV-SM-BLU-1-1-VO-L1-SO-OO
                        leakFault:true
                        pressureFault:0
                     */
                    valveData.manifoldId = manifold_id;
                    valveData.stationId = valve_id;
                    valveData.valveSerialId = serial_number(valve_id, manifold_id);
                    valveData.partNumber = part_number(valve_id, manifold_id);
                    valveData.cycles = lifecycle_count(valve_id, manifold_id) + ++tick_;

                    int index = valveData.cycles % 15;
                    System.out.println("index: " + index);

                    // fault detect
                    boolean can_fault = (manifold_id == 5);
                    if (can_fault) {
                        valveData.leakFault = leakFaultArray[index];
                        valveData.pressureFault = PresureFault.from_int(pressureFaultArray[index]);
                        valveData.valveFault = valveFaultArray[index];
                    } else {
                        valveData.leakFault = false;
                        valveData.valveFault = false;
                        valveData.pressureFault = PresureFault.NO_FAULT;
                    }

                    // data
                    valveData.pressure = pressureArray[index];
                    valveData.suppliedPressure = suppliedPressureArray[index];

                    // init
                    valveData.durationLast12 = 0;
                    valveData.durationLast14 = 1941973408;
                    valveData.equalizationAveragePressure = 32767;
                    valveData.residualOfDynamicAnalysis = 32767;
                    valveData.residualOfDynamicAnalysis = 32767;


                    printValveData(valveData);

                    int handle = valveDataDataWriter.register_instance(valveData);
                    int write_rc = valveDataDataWriter.write(valveData, handle);
                    switch (write_rc) {
                        case RETCODE_OK.value:
                            try { Thread.sleep(100); } catch (InterruptedException ie) { }
                            break;
                        case RETCODE_TIMEOUT.value:
                            System.err.println("ERROR: received received DDS::RETCODE_TIMEOUT!");
                            break;
                        default:
                            System.out.println("write_rc:" + write_rc);
                            run_rc = false;
                            break;
                    }

                }

                num_data += 1;
            }
            System.out.println("Published " + num_data + " samples");
            try { Thread.sleep(1000); } catch (InterruptedException ie) { }

        }

        // Clean up
        domainParticipant.delete_contained_entities();
        domainParticipantFactory.delete_participant(domainParticipant);
        TheServiceParticipant.shutdown();

        System.out.println("Publisher exiting");
    }

}

//        ValveStatus valveStatus = new ValveStatus();
//        valveStatus.valveSerialNumber = 99; // key field
//        valveStatus.timeStamp = 0;
//        valveStatus.stationNumber = 0;
//        valveStatus.cycleCountLimit = 100;
//        valveStatus.cycleCount = 0;
//        valveStatus.pressurePoint = 0.0f;
//        valveStatus.pressureFault = PressureFaultType._PRESSURE_FAULT_N;
//        valveStatus.detectedLeak = LeakDetectedType._LEAK_DETECTED_N;
//        valveStatus.input = InputType._INPUT_N;
//        int handle = valveStatusDataWriter.register_instance(valveStatus); // register key field
//
//        int ret = RETCODE_TIMEOUT.value;
//        for (; valveStatus.cycleCount < N_MSGS; ++valveStatus.cycleCount) {
//            while ((ret = valveStatusDataWriter.write(valveStatus, handle)) == RETCODE_TIMEOUT.value) {
//            }
//            if (ret != RETCODE_OK.value) {
//                System.err.println("ERROR " + valveStatus.cycleCount + " write() returned " + ret);
//            }
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException ie) {
//            }
//        }
//
//
//        if (waitForAcks) {
//            System.out.println("Publisher waiting for acks");
//
//            // Wait for acknowledgements
//            Duration_t forever = new Duration_t(DURATION_INFINITE_SEC.value, DURATION_INFINITE_NSEC.value);
//            dataWriter.wait_for_acknowledgments(forever);
//        } else {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException ie) {
//
//            }
//        }
//        System.out.println("Stop Publisher");
