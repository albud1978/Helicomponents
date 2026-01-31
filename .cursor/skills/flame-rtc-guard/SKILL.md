---
name: flame-rtc-guard
description: Enforces FLAME GPU RTC safety rules and checks. Use when editing RTC kernels, RTC layers, RTC caches, or messaging RTC modules. Applies to NVRTC/JIT logs, MacroProperty sizing, RTC_MAX_FRAMES/MAX_DAYS, Float64 restrictions, and read/write hazards.
---

# FLAME RTC Guard

## Scope
Use this skill when changing RTC kernels, RTC layers, RTC caches, or related messaging RTC modules.

## Mandatory Rules
1. **NVRTC/JIT logs must be clean**  
   - No warnings allowed. If warnings appear, stop and fix before continuing.
2. **No printf in RTC without explicit approval**  
   - Device-side logging is forbidden unless the user explicitly согласовал.
3. **No Float64 anywhere**  
   - Use UInt8/16/32 and Float32 only.
4. **Verify constants and sizes**  
   - Check `RTC_MAX_FRAMES`, `MAX_DAYS`, and `MacroProperty` sizes for consistency.
5. **No read+write in the same layer for the same storage**  
   - Avoid simultaneous read/write on the same `MacroProperty` or shared buffer in one layer.

## Quick Checklist
- [ ] NVRTC log checked and contains zero warnings
- [ ] No `printf` in RTC code (unless explicitly approved)
- [ ] No Float64 types introduced
- [ ] `RTC_MAX_FRAMES`, `MAX_DAYS`, and MacroProperty sizes validated
- [ ] No read+write hazard in a single layer
