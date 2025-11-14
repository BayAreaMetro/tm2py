# 📊 CTRAMP Output Files

!!! success "📖 Complete Documentation Available"
    **For detailed field-level specifications, modern styling, and comprehensive data dictionaries:**
    
    **➡️ [View CTRAMP Output File Specifications](../ctramp-outputs/index.md)**
    
    The new documentation includes:
    - ✅ **Verified field structures** (57 individual tour fields, 18 joint trip fields, etc.)
    - 🎯 **Complete data dictionaries** with 17-mode transportation system
    - 📱 **Modern interactive design** with tabs and visual organization
    - 🔍 **Search and filtering capabilities**
    - 📊 **Real data verification** from 2015-tm22-dev-sprint-04 model run

## 🗂️ Quick File Reference

TM2 produces comprehensive tour and trip microsimulation outputs for both individual and joint travel:

### 📁 **Main Output Files**

=== "📋 Core Files"
    
    | File | Description | Documentation |
    |------|-------------|---------------|
    | `householdData_[iter].csv` | Household demographics | [📄 Details](../ctramp-outputs/household.md) |
    | `personData_[iter].csv` | Individual characteristics | [📄 Details](../ctramp-outputs/person.md) |
    | `wsLocResults_[iter].csv` | Work/school locations | [📄 Details](../ctramp-outputs/workplace-school-location.md) |

=== "🚗 Travel Files"
    
    | File | Description | Documentation |
    |------|-------------|---------------|
    | `indivTourData_[iter].csv` | Individual tours (57 fields) | [📄 Details](../ctramp-outputs/individual-tours.md) |
    | `indivTripData_[iter].csv` | Individual trips (19 fields) | [📄 Details](../ctramp-outputs/individual-trips.md) |
    | `jointTourData_[iter].csv` | Joint household tours (51 fields) | [📄 Details](../ctramp-outputs/joint-tours.md) |
    | `jointTripData_[iter].csv` | Joint household trips (18 fields) | [📄 Details](../ctramp-outputs/joint-trips.md) |

=== "🔧 Supporting Files"
    
    | File | Description |
    |------|-------------|
    | `accessibilities.csv` | Zone accessibility measures |
    | `aoResults.csv` | Auto ownership model results |
    | `ShadowPricingOutput_[type]_0.csv` | Capacity constraint adjustments |

## 🎯 **Key Documentation Features**

!!! tip "Why Use The New Documentation?"
    
    === "✅ Data Accuracy"
        All field counts and structures verified against real model output files
    
    === "🎨 Modern Design"  
        Interactive tabs, search functionality, and mobile-responsive layout
    
    === "📚 Complete Reference"
        Comprehensive mode dictionaries, purpose classifications, and usage examples
    
    === "🔗 Easy Navigation"
        Cross-linked files with clear relationships and dependencies

## 📖 **Access Full Documentation**

**[👉 Visit CTRAMP Output File Specifications](../ctramp-outputs/index.md)**