/* Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package flume;

import (
        "thrift"
        "fmt"
)



type Status int
const (
  OK Status = 0
  FAILED Status = 1
  ERROR Status = 2
  UNKNOWN Status = 3
)
func (p Status) String() string {
  switch p {
  case OK: return "OK"
  case FAILED: return "FAILED"
  case ERROR: return "ERROR"
  case UNKNOWN: return "UNKNOWN"
  }
  return ""
}

func FromStatusString(s string) Status {
  switch s {
  case "OK": return OK
  case "FAILED": return FAILED
  case "ERROR": return ERROR
  case "UNKNOWN": return UNKNOWN
  }
  return Status(-10000)
}

func (p Status) Value() int {
  return int(p)
}

func (p Status) IsEnum() bool {
  return true
}

/**
 * Attributes:
 *  - Headers
 *  - Body
 */
type ThriftFlumeEvent struct {
  thrift.TStruct
  Headers thrift.TMap "headers"; // 1
  Body string "body"; // 2
}

func NewThriftFlumeEvent() *ThriftFlumeEvent {
  output := &ThriftFlumeEvent{
    TStruct:thrift.NewTStruct("ThriftFlumeEvent", []thrift.TField{
    thrift.NewTField("headers", thrift.MAP, 1),
    thrift.NewTField("body", thrift.STRING, 2),
    }),
  }
  {
  }
  return output
}

func (p *ThriftFlumeEvent) Read(iprot thrift.TProtocol) (err thrift.TProtocolException) {
  _, err = iprot.ReadStructBegin()
  if err != nil { return thrift.NewTProtocolExceptionReadStruct(p.ThriftName(), err); }
  for {
    fieldName, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if fieldId < 0 {
      fieldId = int16(p.FieldIdFromFieldName(fieldName))
    } else if fieldName == "" {
      fieldName = p.FieldNameFromFieldId(int(fieldId))
    }
    if fieldTypeId == thrift.GENERIC {
      fieldTypeId = p.FieldFromFieldId(int(fieldId)).TypeId()
    }
    if err != nil {
      return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    if fieldId == 1 || fieldName == "headers" {
      if fieldTypeId == thrift.MAP {
        err = p.ReadField1(iprot)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      } else if fieldTypeId == thrift.VOID {
        err = iprot.Skip(fieldTypeId)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      } else {
        err = p.ReadField1(iprot)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      }
    } else if fieldId == 2 || fieldName == "body" {
      if fieldTypeId == thrift.STRING {
        err = p.ReadField2(iprot)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      } else if fieldTypeId == thrift.VOID {
        err = iprot.Skip(fieldTypeId)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      } else {
        err = p.ReadField2(iprot)
        if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
      }
    } else {
      err = iprot.Skip(fieldTypeId)
      if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
    }
    err = iprot.ReadFieldEnd()
    if err != nil { return thrift.NewTProtocolExceptionReadField(int(fieldId), fieldName, p.ThriftName(), err); }
  }
  err = iprot.ReadStructEnd()
  if err != nil { return thrift.NewTProtocolExceptionReadStruct(p.ThriftName(), err); }
  return err
}

func (p *ThriftFlumeEvent) ReadField1(iprot thrift.TProtocol) (err thrift.TProtocolException) {
  _ktype3, _vtype4, _size2, err := iprot.ReadMapBegin()
  if err != nil {
    return thrift.NewTProtocolExceptionReadField(-1, "p.Headers", "", err)
  }
  p.Headers = thrift.NewTMap(_ktype3, _vtype4, _size2)
  for _i6:= 0; _i6 < _size2; _i6++ {
    v9, err10 := iprot.ReadString()
    if err10 != nil { return thrift.NewTProtocolExceptionReadField(0, "_key7", "", err10); }
    _key7 := v9
    v11, err12 := iprot.ReadString()
    if err12 != nil { return thrift.NewTProtocolExceptionReadField(0, "_val8", "", err12); }
    _val8 := v11
    p.Headers.Set(_key7, _val8)
  }
  err = iprot.ReadMapEnd()
  if err != nil { return thrift.NewTProtocolExceptionReadField(-1, "", "map", err); }
  return err
}

func (p *ThriftFlumeEvent) ReadFieldHeaders(iprot thrift.TProtocol) (thrift.TProtocolException) {
  return p.ReadField1(iprot)
}

func (p *ThriftFlumeEvent) ReadField2(iprot thrift.TProtocol) (err thrift.TProtocolException) {
  v13, err14 := iprot.ReadString()
  if err14 != nil { return thrift.NewTProtocolExceptionReadField(2, "body", p.ThriftName(), err14); }
  p.Body = v13
  return err
}

func (p *ThriftFlumeEvent) ReadFieldBody(iprot thrift.TProtocol) (thrift.TProtocolException) {
  return p.ReadField2(iprot)
}

func (p *ThriftFlumeEvent) Write(oprot thrift.TProtocol) (err thrift.TProtocolException) {
  err = oprot.WriteStructBegin("ThriftFlumeEvent")
  if err != nil { return thrift.NewTProtocolExceptionWriteStruct(p.ThriftName(), err); }
  err = p.WriteField1(oprot)
  if err != nil { return err }
  err = p.WriteField2(oprot)
  if err != nil { return err }
  err = oprot.WriteFieldStop()
  if err != nil { return thrift.NewTProtocolExceptionWriteField(-1, "STOP", p.ThriftName(), err); }
  err = oprot.WriteStructEnd()
  if err != nil { return thrift.NewTProtocolExceptionWriteStruct(p.ThriftName(), err); }
  return err
}

func (p *ThriftFlumeEvent) WriteField1(oprot thrift.TProtocol) (err thrift.TProtocolException) {
  if p.Headers != nil {
    err = oprot.WriteFieldBegin("headers", thrift.MAP, 1)
    if err != nil { return thrift.NewTProtocolExceptionWriteField(1, "headers", p.ThriftName(), err); }
    err = oprot.WriteMapBegin(thrift.STRING, thrift.STRING, p.Headers.Len())
    if err != nil { return thrift.NewTProtocolExceptionWriteField(-1, "", "map", err); }
    for Miter15 := range p.Headers.Iter() {
      Kiter16, Viter17 := Miter15.Key().(string), Miter15.Value().(string)
      err = oprot.WriteString(string(Kiter16))
      if err != nil { return thrift.NewTProtocolExceptionWriteField(0, "Kiter16", "", err); }
      err = oprot.WriteString(string(Viter17))
      if err != nil { return thrift.NewTProtocolExceptionWriteField(0, "Viter17", "", err); }
    }
    err = oprot.WriteMapEnd()
    if err != nil { return thrift.NewTProtocolExceptionWriteField(-1, "", "map", err); }
    err = oprot.WriteFieldEnd()
    if err != nil { return thrift.NewTProtocolExceptionWriteField(1, "headers", p.ThriftName(), err); }
  }
  return err
}

func (p *ThriftFlumeEvent) WriteFieldHeaders(oprot thrift.TProtocol) (thrift.TProtocolException) {
  return p.WriteField1(oprot)
}

func (p *ThriftFlumeEvent) WriteField2(oprot thrift.TProtocol) (err thrift.TProtocolException) {
  err = oprot.WriteFieldBegin("body", thrift.STRING, 2)
  if err != nil { return thrift.NewTProtocolExceptionWriteField(2, "body", p.ThriftName(), err); }
  err = oprot.WriteString(string(p.Body))
  if err != nil { return thrift.NewTProtocolExceptionWriteField(2, "body", p.ThriftName(), err); }
  err = oprot.WriteFieldEnd()
  if err != nil { return thrift.NewTProtocolExceptionWriteField(2, "body", p.ThriftName(), err); }
  return err
}

func (p *ThriftFlumeEvent) WriteFieldBody(oprot thrift.TProtocol) (thrift.TProtocolException) {
  return p.WriteField2(oprot)
}

func (p *ThriftFlumeEvent) TStructName() string {
  return "ThriftFlumeEvent"
}

func (p *ThriftFlumeEvent) ThriftName() string {
  return "ThriftFlumeEvent"
}

func (p *ThriftFlumeEvent) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("ThriftFlumeEvent(%+v)", *p)
}

func (p *ThriftFlumeEvent) CompareTo(other interface{}) (int, bool) {
  if other == nil {
    return 1, true
  }
  data, ok := other.(*ThriftFlumeEvent)
  if !ok {
    return 0, false
  }
  if cmp, ok := p.Headers.CompareTo(data.Headers); !ok || cmp != 0 {
    return cmp, ok
  }
  if p.Body != data.Body {
    if p.Body < data.Body {
      return -1, true
    }
    return 1, true
  }
  return 0, true
}

func (p *ThriftFlumeEvent) AttributeByFieldId(id int) interface{} {
  switch id {
  default: return nil
  case 1: return p.Headers
  case 2: return p.Body
  }
  return nil
}

func (p *ThriftFlumeEvent) TStructFields() thrift.TFieldContainer {
  return thrift.NewTFieldContainer([]thrift.TField{
    thrift.NewTField("headers", thrift.MAP, 1),
    thrift.NewTField("body", thrift.STRING, 2),
    })
}

func init() {
}

