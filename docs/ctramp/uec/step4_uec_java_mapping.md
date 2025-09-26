
# CT-RAMP UEC File to Java Class Mapping
**Generated:** 2025-09-26 07:56:53
**Purpose:** Map Excel UEC files to Java processor classes for Python port

## Executive Summary

- **Total UEC files analyzed:** 31
- **Java classes available:** 807
- **Successful mappings:** 25
- **High confidence mappings:** 21

## UEC File Mappings

### Accessibilities.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\Accessibilities.xls`
- Size: 0.10 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **AccessibilitiesDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 194 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**[HIGH]** **MandatoryAccessibilitiesDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 206 lines
- UEC Processor: YES
- Model Component: mandatory_tour
- UEC Imports: com.pb.common.calculator.VariableTable

**RECOMMENDED:** `AccessibilitiesDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### AtWorkSubtourFrequency.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\AtWorkSubtourFrequency.xls`
- Size: 0.06 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **SandagAtWorkSubtourFrequencyDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 64 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**[HIGH]** **AtWorkSubtourFrequencyDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 203 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **HouseholdAtWorkSubtourFrequencyModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 333 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**RECOMMENDED:** `SandagAtWorkSubtourFrequencyDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### AutoOwnership.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\AutoOwnership.xls`
- Size: 0.20 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **SandagAutoOwnershipChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 120 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[HIGH]** **AutoOwnershipChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 320 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **HouseholdAutoOwnershipModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 330 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.MandatoryAccessibilitiesCalculator

**[MED]** **TourVehicleTypeChoiceModel** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 191 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.MandatoryAccessibilitiesCalculator

**RECOMMENDED:** `AutoOwnershipChoiceDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### BestTransitPathUtility.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\BestTransitPathUtility.xls`
- Size: 0.15 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **TransitPath** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 45 lines
- UEC Processor: YES
- Model Component: tour_mode_choice

**[HIGH]** **Util** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 115 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **be** (0.8 confidence)
- Package: `org.jppf.utils`
- Size: 27 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `TransitPath` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### CoordinatedDailyActivityPattern.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\CoordinatedDailyActivityPattern.xls`
- Size: 0.35 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **SandagCoordinatedDailyActivityPatternDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 168 lines
- UEC Processor: NO
- Model Component: accessibility

**[HIGH]** **CoordinatedDailyActivityPatternDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 428 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **act** (0.8 confidence)
- Package: `org.jppf.server.nio.multiplexer.generic`
- Size: 63 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `in` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### DestinationChoiceAlternativeSample.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\DestinationChoiceAlternativeSample.xls`
- Size: 0.09 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **Alternative** (0.8 confidence)
- Package: `com.pb.common.newmodel`
- Size: 118 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**RECOMMENDED:** `Alternative` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### JointTourFrequency.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\JointTourFrequency.xls`
- Size: 0.08 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[MED]** **MTCTM2TourBasedModel** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 314 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[MED]** **SandagAtWorkSubtourFrequencyDMU** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 64 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**RECOMMENDED:** `in` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### MandatoryAccess.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\MandatoryAccess.xls`
- Size: 0.05 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **and** (0.8 confidence)
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **MandatoryAccessibilitiesDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.accessibilities`
- Size: 206 lines
- UEC Processor: YES
- Model Component: mandatory_tour
- UEC Imports: com.pb.common.calculator.VariableTable

**RECOMMENDED:** `and` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### MandatoryTourFrequency.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\MandatoryTourFrequency.xls`
- Size: 0.06 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **and** (0.8 confidence)
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **HouseholdIndividualMandatoryTourFrequencyModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 404 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.MandatoryAccessibilitiesCalculator

**[HIGH]** **HouseholdIndividualNonMandatoryTourFrequencyModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 773 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.MandatoryAccessibilitiesCalculator

**[HIGH]** **IndividualMandatoryTourFrequencyDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 291 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**RECOMMENDED:** `and` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### NonMandatoryIndividualTourFrequency.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\NonMandatoryIndividualTourFrequency.xls`
- Size: 0.18 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **and** (0.8 confidence)
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[MED]** **MTCTM2TourBasedModel** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 314 lines
- UEC Processor: NO
- Model Component: population_synthesis

**RECOMMENDED:** `and` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### ParkingProvision.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\ParkingProvision.xls`
- Size: 0.03 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **is** (0.8 confidence)
- Package: `org.jppf.server.app`
- Size: 224 lines
- UEC Processor: NO
- Model Component: stop_frequency

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **SandagParkingProvisionChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 85 lines
- UEC Processor: NO
- Model Component: mandatory_tour

**[HIGH]** **ParkingProvisionChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 233 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **ParkingProvisionModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 201 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**RECOMMENDED:** `in` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### StopFrequency.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\StopFrequency.xls`
- Size: 0.13 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **SandagStopFrequencyDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 250 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **Stop** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 199 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[MED]** **SandagAtWorkSubtourFrequencyDMU** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 64 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**[MED]** **SandagStopLocationDMU** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 114 lines
- UEC Processor: NO
- Model Component: joint_tour

**RECOMMENDED:** `SandagStopFrequencyDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TazDistance.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TazDistance.xls`
- Size: 0.05 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **is** (0.8 confidence)
- Package: `org.jppf.server.app`
- Size: 224 lines
- UEC Processor: NO
- Model Component: stop_frequency

**[HIGH]** **DestChoiceTwoStageSoaTazDistanceUtilityDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 176 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**RECOMMENDED:** `DestChoiceTwoStageSoaTazDistanceUtilityDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourDcSoaDistance.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourDcSoaDistance.xls`
- Size: 0.05 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **is** (0.8 confidence)
- Package: `org.jppf.server.app`
- Size: 224 lines
- UEC Processor: NO
- Model Component: stop_frequency

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**RECOMMENDED:** `Tour` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourDcSoaDistanceNoSchoolSize.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourDcSoaDistanceNoSchoolSize.xls`
- Size: 0.06 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **Os** (0.8 confidence)
- Package: `com.pb.common.env`
- Size: 199 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **is** (0.8 confidence)
- Package: `org.jppf.server.app`
- Size: 224 lines
- UEC Processor: NO
- Model Component: stop_frequency

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**RECOMMENDED:** `Tour` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourDepartureAndDuration.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourDepartureAndDuration.xls`
- Size: 0.33 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **and** (0.8 confidence)
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **HouseholdIndividualMandatoryTourDepartureAndDurationTime** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1550 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable

**[HIGH]** **NonMandatoryTourDepartureAndDurationTime** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 1213 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.MatrixDataServerIf, com.pb.common.calculator.VariableTable, com.pb.common.calculator.MatrixDataManager

**[HIGH]** **SubtourDepartureAndDurationTime** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 849 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.MatrixDataServerIf, com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.MandatoryAccessibilitiesCalculator

**RECOMMENDED:** `and` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourDestinationChoice.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourDestinationChoice.xls`
- Size: 0.32 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[MED]** **ChoiceModelApplication** (0.7 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 649 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable, com.pb.common.newmodel.UtilityExpressionCalculator

**[MED]** **ChoiceStrategy** (0.7 confidence)
- Package: `com.pb.common.model`
- Size: 32 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `in` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourDestinationChoice2.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourDestinationChoice2.xls`
- Size: 0.28 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **in** (0.8 confidence)
- Package: `com.pb.common.util`
- Size: 135 lines
- UEC Processor: YES
- Model Component: None

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**RECOMMENDED:** `in` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TourModeChoice.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TourModeChoice.xls`
- Size: 1.15 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **SandagTourModeChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 383 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[HIGH]** **Tour** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 770 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **TourModeChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 481 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **TourModeChoiceModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 529 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.AutoAndNonMotorizedSkimsCalculator

**RECOMMENDED:** `Tour` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TransitSubsidyAndPass.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TransitSubsidyAndPass.xls`
- Size: 0.11 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **and** (0.8 confidence)
- Package: `com.pb.common.model.tests`
- Size: 172 lines
- UEC Processor: YES
- Model Component: population_synthesis

**[HIGH]** **SandagTransitSubsidyAndPassDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 76 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[HIGH]** **TransitSubsidyAndPassDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 193 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[HIGH]** **TransitSubsidyAndPassModel** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 544 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.VariableTable, com.pb.mtctm2.abm.accessibilities.AutoAndNonMotorizedSkimsCalculator, com.pb.mtctm2.abm.accessibilities.AutoTazSkimsCalculator

**RECOMMENDED:** `and` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### TripModeChoice.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TripModeChoice.xls`
- Size: 0.49 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **SandagTripModeChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.application`
- Size: 464 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues

**[HIGH]** **TripModeChoiceDMU** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 737 lines
- UEC Processor: YES
- Model Component: population_synthesis
- UEC Imports: com.pb.common.calculator.IndexValues, com.pb.common.calculator.VariableTable

**[MED]** **ModelAlternative** (0.7 confidence)
- Package: `com.pb.common.calculator2`
- Size: 36 lines
- UEC Processor: YES
- Model Component: None

**[MED]** **ModelEntry** (0.7 confidence)
- Package: `com.pb.common.calculator2`
- Size: 41 lines
- UEC Processor: YES
- Model Component: None

**[MED]** **ModelHeader** (0.7 confidence)
- Package: `com.pb.common.calculator2`
- Size: 50 lines
- UEC Processor: YES
- Model Component: None

**RECOMMENDED:** `SandagTripModeChoiceDMU` (0.8 confidence)

**Notes:** Multiple high-confidence matches found

---

### AutoSkims.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\AutoSkims.xls`
- Size: 0.10 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **Os** (0.8 confidence)
- Package: `com.pb.common.env`
- Size: 199 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `to` (0.8 confidence)

**Notes:** No clear UEC processor identified

---

### ParkLocationChoice.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\ParkLocationChoice.xls`
- Size: 0.11 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **Location** (0.8 confidence)
- Package: `org.jppf.server.protocol`
- Size: 78 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `Location` (0.8 confidence)

**Notes:** No clear UEC processor identified

---

### SlcSoaDistanceUtility.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\SlcSoaDistanceUtility.xls`
- Size: 0.03 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **is** (0.8 confidence)
- Package: `org.jppf.server.app`
- Size: 224 lines
- UEC Processor: NO
- Model Component: stop_frequency

**[HIGH]** **Util** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 115 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `is` (0.8 confidence)

**Notes:** No clear UEC processor identified

---

### StopLocationChoice.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\StopLocationChoice.xls`
- Size: 0.10 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**Java Class Matches:**

**[HIGH]** **to** (0.8 confidence)
- Package: `org.jppf.node.event`
- Size: 33 lines
- UEC Processor: NO
- Model Component: None

**[HIGH]** **Stop** (0.8 confidence)
- Package: `com.pb.mtctm2.abm.ctramp`
- Size: 199 lines
- UEC Processor: NO
- Model Component: population_synthesis

**[HIGH]** **Location** (0.8 confidence)
- Package: `org.jppf.server.protocol`
- Size: 78 lines
- UEC Processor: NO
- Model Component: None

**RECOMMENDED:** `to` (0.8 confidence)

**Notes:** No clear UEC processor identified

---

### Accessibilities_DC.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\Accessibilities_DC.xls`
- Size: 0.02 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

### DriveTransitWalkSkims.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\DriveTransitWalkSkims.xls`
- Size: 0.07 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

### SlcSoaSize.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\SlcSoaSize.xls`
- Size: 0.05 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

### TransponderOwnership.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\TransponderOwnership.xls`
- Size: 0.02 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

### WalkTransitDriveSkims.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\WalkTransitDriveSkims.xls`
- Size: 0.07 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

### WalkTransitWalkSkims.xls

**Excel File Details:**
- Path: `C:\GitHub\travel-model-two\model-files\model\WalkTransitWalkSkims.xls`
- Size: 0.06 MB
- Component: Unknown
- Error: Could not read Excel file: openpyxl does not support the old .xls file format, please use xlrd to re...

**No Java class matches found**

**Notes:** No Java class matches found

---

## Python Port Implementation Guide

### Phase 1: High Confidence Mappings (Ready for Port)

- **Accessibilities.xls** -> `AccessibilitiesDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.accessibilities`
  - Lines: 194

- **AtWorkSubtourFrequency.xls** -> `SandagAtWorkSubtourFrequencyDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.application`
  - Lines: 64

- **AutoOwnership.xls** -> `AutoOwnershipChoiceDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.ctramp`
  - Lines: 320

- **BestTransitPathUtility.xls** -> `TransitPath`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.accessibilities`
  - Lines: 45

- **CoordinatedDailyActivityPattern.xls** -> `in`
  - Confidence: 0.8
  - Package: `com.pb.common.util`
  - Lines: 135

- **DestinationChoiceAlternativeSample.xls** -> `Alternative`
  - Confidence: 0.8
  - Package: `com.pb.common.newmodel`
  - Lines: 118

- **JointTourFrequency.xls** -> `in`
  - Confidence: 0.8
  - Package: `com.pb.common.util`
  - Lines: 135

- **MandatoryAccess.xls** -> `and`
  - Confidence: 0.8
  - Package: `com.pb.common.model.tests`
  - Lines: 172

- **MandatoryTourFrequency.xls** -> `and`
  - Confidence: 0.8
  - Package: `com.pb.common.model.tests`
  - Lines: 172

- **NonMandatoryIndividualTourFrequency.xls** -> `and`
  - Confidence: 0.8
  - Package: `com.pb.common.model.tests`
  - Lines: 172

- **ParkingProvision.xls** -> `in`
  - Confidence: 0.8
  - Package: `com.pb.common.util`
  - Lines: 135

- **StopFrequency.xls** -> `SandagStopFrequencyDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.application`
  - Lines: 250

- **TazDistance.xls** -> `DestChoiceTwoStageSoaTazDistanceUtilityDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.ctramp`
  - Lines: 176

- **TourDcSoaDistance.xls** -> `Tour`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.ctramp`
  - Lines: 770

- **TourDcSoaDistanceNoSchoolSize.xls** -> `Tour`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.ctramp`
  - Lines: 770

- **TourDepartureAndDuration.xls** -> `and`
  - Confidence: 0.8
  - Package: `com.pb.common.model.tests`
  - Lines: 172

- **TourDestinationChoice.xls** -> `in`
  - Confidence: 0.8
  - Package: `com.pb.common.util`
  - Lines: 135

- **TourDestinationChoice2.xls** -> `in`
  - Confidence: 0.8
  - Package: `com.pb.common.util`
  - Lines: 135

- **TourModeChoice.xls** -> `Tour`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.ctramp`
  - Lines: 770

- **TransitSubsidyAndPass.xls** -> `and`
  - Confidence: 0.8
  - Package: `com.pb.common.model.tests`
  - Lines: 172

- **TripModeChoice.xls** -> `SandagTripModeChoiceDMU`
  - Confidence: 0.8
  - Package: `com.pb.mtctm2.abm.application`
  - Lines: 464

### Phase 2: Medium Confidence Mappings (Needs Investigation)

### Phase 3: Low Confidence / Manual Review Required

- **AutoSkims.xls** -> Manual analysis required
- **ParkLocationChoice.xls** -> Manual analysis required
- **SlcSoaDistanceUtility.xls** -> Manual analysis required
- **StopLocationChoice.xls** -> Manual analysis required
- **Accessibilities_DC.xls** -> Manual analysis required
- **DriveTransitWalkSkims.xls** -> Manual analysis required
- **SlcSoaSize.xls** -> Manual analysis required
- **TransponderOwnership.xls** -> Manual analysis required
- **WalkTransitDriveSkims.xls** -> Manual analysis required
- **WalkTransitWalkSkims.xls** -> Manual analysis required

## Next Steps for Step 5

Based on this mapping analysis:

1. **Immediate Port Candidates:** 21 files with clear Java processors
2. **Investigation Needed:** 0 files requiring deeper analysis  
3. **Manual Analysis:** 10 files needing manual review

### Key Java Packages for Python Port:
- `com.pb.common.calculator2` - Core UEC processing framework
- `com.pb.mtctm2.abm.ctramp` - Main CT-RAMP model classes
- `com.pb.common.datafile` - Data input/output handling

---
*This mapping provides the foundation for systematic Python porting in Step 5.*
