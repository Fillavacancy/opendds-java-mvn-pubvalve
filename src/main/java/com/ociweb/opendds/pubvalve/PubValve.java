package com.ociweb.opendds.pubvalve;

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
            String nativeJarName  = null;
            if (osName.contains("mac")) {
                nativeJarName = "OpenDDSDarwin.jar";
            } else if (osName.contains("linux")) {
                nativeJarName = "OpenDDSLinux.jar";
            } else if (osName.contains("Windows")) {
                nativeJarName = "OpenDDSWindows.jar";
            }

            if (nativeJarName == null) {
                throw new UnsupportedOperationException("No known OpenDDS native jar for OS "+ osName);
            } else {
                System.out.println( nativeJarName + " contains native libaries for " + osName);
            }

            final String currentWorkingDirString = Paths.get("").toAbsolutePath().normalize().toString();
            final Path jarFilePath = Paths.get(currentWorkingDirString, nativeJarName);
            Files.deleteIfExists(jarFilePath);
            final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(nativeJarName);
            Files.copy(stream, jarFilePath);
            stream.close();

            if(Files.exists(jarFilePath)) {
                // unpack Jar to cwd
                Map<String, String> libFileNameMap = new HashMap<>();
                JarFile jar = new JarFile(jarFilePath.toString());
                for (Enumeration<JarEntry> enumEntries = jar.entries(); enumEntries.hasMoreElements();) {
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
                for (String lib: libs) {
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

    public static void main(String[] args) {

        System.out.println("Start Valve Publisher");
        boolean reliable = checkReliable(args);
        boolean waitForAcks = checkWaitForAcks(args);

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

        ValveStatusTypeSupportImpl valveStatusTypeSupport = new ValveStatusTypeSupportImpl();
        if (valveStatusTypeSupport.register_type(domainParticipant, "") != RETCODE_OK.value) {
            System.err.println("ERROR: register_type failed");
            return;
        }

        Topic topic = domainParticipant.create_topic(VALVE_TOPIC,
                valveStatusTypeSupport.get_type_name(),
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

        ValveStatusDataWriter valveStatusDataWriter = ValveStatusDataWriterHelper.narrow(dataWriter);

        ValveStatus valveStatus = new ValveStatus();
        valveStatus.valveSerialNumber = 99; // key field
        valveStatus.timeStamp = 0;
        valveStatus.stationNumber = 0;
        valveStatus.cycleCountLimit = 100;
        valveStatus.cycleCount = 0;
        valveStatus.pressurePoint = 0.0f;
        valveStatus.pressureFault = PressureFaultType._PRESSURE_FAULT_N;
        valveStatus.detectedLeak = LeakDetectedType._LEAK_DETECTED_N;
        valveStatus.input = InputType._INPUT_N;
        int handle = valveStatusDataWriter.register_instance(valveStatus); // register key field

        int ret = RETCODE_TIMEOUT.value;
        for (; valveStatus.cycleCount < N_MSGS; ++valveStatus.cycleCount) {
            while ((ret = valveStatusDataWriter.write(valveStatus, handle)) == RETCODE_TIMEOUT.value) {
            }
            if (ret != RETCODE_OK.value) {
                System.err.println("ERROR " + valveStatus.cycleCount + " write() returned " + ret);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
        }

        if (waitForAcks) {
            System.out.println("Publisher waiting for acks");

            // Wait for acknowledgements
            Duration_t forever = new Duration_t(DURATION_INFINITE_SEC.value, DURATION_INFINITE_NSEC.value);
            dataWriter.wait_for_acknowledgments(forever);
        } else {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {

            }
        }
        System.out.println("Stop Publisher");

        // Clean up
        domainParticipant.delete_contained_entities();
        domainParticipantFactory.delete_participant(domainParticipant);
        TheServiceParticipant.shutdown();

        System.out.println("Publisher exiting");
    }

}
