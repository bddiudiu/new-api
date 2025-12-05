package controller

import (
	"net/http"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

// Sign 用户签到
func Sign(c *gin.Context) {
	userId := c.GetInt("id")
	if userId == 0 {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": "用户未登录",
		})
		return
	}

	result, err := model.DoSign(userId)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": result.Success,
		"message": result.Message,
		"data": gin.H{
			"quota": result.Quota,
		},
	})
}

// GetSignList 获取用户签到列表
func GetSignList(c *gin.Context) {
	userId := c.GetInt("id")
	if userId == 0 {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": "用户未登录",
		})
		return
	}

	signList, err := model.GetUserSignList(userId)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "",
		"data":    signList,
	})
}

// GetSignInfo 获取用户签到信息
func GetSignInfo(c *gin.Context) {
	userId := c.GetInt("id")
	if userId == 0 {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": "用户未登录",
		})
		return
	}

	signInfo, err := model.GetUserSignInfo(userId)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "",
		"data":    signInfo,
	})
}
