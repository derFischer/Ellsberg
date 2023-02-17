package interpreter

import (
	"encoding/binary"
	"fmt"

	"github.com/gogo/protobuf/proto"
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/raft/raftpb"
)

type PbMessageType int8

const (
	RangeResponse                    PbMessageType = 0
	PutResponse                      PbMessageType = 1
	DeleteRangeResponse              PbMessageType = 2
	TxnResponse                      PbMessageType = 3
	CompactionResponse               PbMessageType = 4
	HashKVResponse                   PbMessageType = 5
	HashResponse                     PbMessageType = 6
	SnapshotResponse                 PbMessageType = 7
	WatchResponse                    PbMessageType = 8
	LeaseGrantResponse               PbMessageType = 9
	LeaseRevokeResponse              PbMessageType = 10
	LeaseCheckpointResponse          PbMessageType = 11
	LeaseKeepAliveResponse           PbMessageType = 12
	LeaseTimeToLiveResponse          PbMessageType = 13
	LeaseLeasesResponse              PbMessageType = 14
	MemberAddResponse                PbMessageType = 15
	MemberRemoveResponse             PbMessageType = 16
	MemberUpdateResponse             PbMessageType = 17
	MemberListResponse               PbMessageType = 18
	MemberPromoteResponse            PbMessageType = 19
	DefragmentResponse               PbMessageType = 20
	MoveLeaderResponse               PbMessageType = 21
	AlarmResponse                    PbMessageType = 22
	DowngradeResponse                PbMessageType = 23
	StatusResponse                   PbMessageType = 24
	AuthEnableResponse               PbMessageType = 25
	AuthDisableResponse              PbMessageType = 26
	AuthStatusResponse               PbMessageType = 27
	AuthenticateResponse             PbMessageType = 28
	AuthUserAddResponse              PbMessageType = 29
	AuthUserGetResponse              PbMessageType = 30
	AuthUserDeleteResponse           PbMessageType = 31
	AuthUserChangePasswordResponse   PbMessageType = 32
	AuthUserGrantRoleResponse        PbMessageType = 33
	AuthUserRevokeRoleResponse       PbMessageType = 34
	AuthRoleAddResponse              PbMessageType = 35
	AuthRoleGetResponse              PbMessageType = 36
	AuthRoleListResponse             PbMessageType = 37
	AuthUserListResponse             PbMessageType = 38
	AuthRoleDeleteResponse           PbMessageType = 39
	AuthRoleGrantPermissionResponse  PbMessageType = 40
	AuthRoleRevokePermissionResponse PbMessageType = 41
)

func EncodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

func PanicIfMarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Marshal failed, err:%v\n", err))
	}
}

func PanicIfUnmarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Unmarshal failed, err:%v\n", err))
	}
}

func MarshalRaftPbMessage(msg *raftpb.Message) []byte {
	if msg == nil {
		return make([]byte, 0)
	} else {
		bytes, err := proto.Marshal(msg)
		PanicIfMarshalError(err)
		return bytes
	}
}

func UnmarshalRaftPbMessage(bytes []byte) *raftpb.Message {
	if len(bytes) == 0 {
		return nil
	} else {
		pbMsg := &raftpb.Message{}
		err := pbMsg.Unmarshal(bytes)
		PanicIfUnmarshalError(err)
		return pbMsg
	}
}

func ClientRespType(resp proto.Message) uint64 {
	if resp == nil {
		return 42
	}
	switch resp.(type) {
	case *pb.RangeResponse:
		return 0
	case *pb.PutResponse:
		return 1
	case *pb.DeleteRangeResponse:
		return 2
	case *pb.TxnResponse:
		return 3
	case *pb.CompactionResponse:
		return 4
	case *pb.HashKVResponse:
		return 5
	case *pb.HashResponse:
		return 6
	case *pb.SnapshotResponse:
		return 7
	case *pb.WatchResponse:
		return 8
	case *pb.LeaseGrantResponse:
		return 9
	case *pb.LeaseRevokeResponse:
		return 10
	case *pb.LeaseCheckpointResponse:
		return 11
	case *pb.LeaseKeepAliveResponse:
		return 12
	case *pb.LeaseTimeToLiveResponse:
		return 13
	case *pb.LeaseLeasesResponse:
		return 14
	case *pb.MemberAddResponse:
		return 15
	case *pb.MemberRemoveResponse:
		return 16
	case *pb.MemberUpdateResponse:
		return 17
	case *pb.MemberListResponse:
		return 18
	case *pb.MemberPromoteResponse:
		return 19
	case *pb.DefragmentResponse:
		return 20
	case *pb.MoveLeaderResponse:
		return 21
	case *pb.AlarmResponse:
		return 22
	case *pb.DowngradeResponse:
		return 23
	case *pb.StatusResponse:
		return 24
	case *pb.AuthEnableResponse:
		return 25
	case *pb.AuthDisableResponse:
		return 26
	case *pb.AuthStatusResponse:
		return 27
	case *pb.AuthenticateResponse:
		return 28
	case *pb.AuthUserAddResponse:
		return 29
	case *pb.AuthUserGetResponse:
		return 30
	case *pb.AuthUserDeleteResponse:
		return 31
	case *pb.AuthUserChangePasswordResponse:
		return 32
	case *pb.AuthUserGrantRoleResponse:
		return 33
	case *pb.AuthUserRevokeRoleResponse:
		return 34
	case *pb.AuthRoleAddResponse:
		return 35
	case *pb.AuthRoleGetResponse:
		return 36
	case *pb.AuthRoleListResponse:
		return 37
	case *pb.AuthUserListResponse:
		return 38
	case *pb.AuthRoleDeleteResponse:
		return 39
	case *pb.AuthRoleGrantPermissionResponse:
		return 40
	case *pb.AuthRoleRevokePermissionResponse:
		return 41
	default:
		panic(fmt.Sprintf("Unknown response type:%v", resp))
	}
}

func UnmarshalClientResp(pbType PbMessageType, data []byte) interface{} {
	switch pbType {
	case 0:
		pbMsg := pb.RangeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 1:
		pbMsg := pb.PutResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 2:
		pbMsg := pb.DeleteRangeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 3:
		pbMsg := pb.TxnResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 4:
		pbMsg := pb.CompactionResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 5:
		pbMsg := pb.HashKVResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 6:
		pbMsg := pb.HashResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 7:
		pbMsg := pb.SnapshotResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 8:
		pbMsg := pb.WatchResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 9:
		pbMsg := pb.LeaseGrantResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 10:
		pbMsg := pb.LeaseRevokeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 11:
		pbMsg := pb.LeaseCheckpointResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 12:
		pbMsg := pb.LeaseKeepAliveResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 13:
		pbMsg := pb.LeaseTimeToLiveResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 14:
		pbMsg := pb.LeaseLeasesResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 15:
		pbMsg := pb.MemberAddResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 16:
		pbMsg := pb.MemberRemoveResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 17:
		pbMsg := pb.MemberUpdateResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 18:
		pbMsg := pb.MemberListResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 19:
		pbMsg := pb.MemberPromoteResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 20:
		pbMsg := pb.DefragmentResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 21:
		pbMsg := pb.MoveLeaderResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 22:
		pbMsg := pb.AlarmResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 23:
		pbMsg := pb.DowngradeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 24:
		pbMsg := pb.StatusResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 25:
		pbMsg := pb.AuthEnableResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 26:
		pbMsg := pb.AuthDisableResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 27:
		pbMsg := pb.AuthStatusResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 28:
		pbMsg := pb.AuthenticateResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 29:
		pbMsg := pb.AuthUserAddResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 30:
		pbMsg := pb.AuthUserGetResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 31:
		pbMsg := pb.AuthUserDeleteResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 32:
		pbMsg := pb.AuthUserChangePasswordResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 33:
		pbMsg := pb.AuthUserGrantRoleResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 34:
		pbMsg := pb.AuthUserRevokeRoleResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 35:
		pbMsg := pb.AuthRoleAddResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 36:
		pbMsg := pb.AuthRoleGetResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 37:
		pbMsg := pb.AuthRoleListResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 38:
		pbMsg := pb.AuthUserListResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 39:
		pbMsg := pb.AuthRoleDeleteResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 40:
		pbMsg := pb.AuthRoleGrantPermissionResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 41:
		pbMsg := pb.AuthRoleRevokePermissionResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 42:
		return nil
	default:
		panic("unknown pbType")
	}
}
